#include <cassert>
#include <algorithm>
#include <string>
#include <iostream>
#include <vector>
#include <sched.h>
#include <unistd.h>
#include <pthread.h>
#include <thread>
#include <chrono>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <getopt.h>
#include <mutex>
#include <condition_variable>

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>


#define SERVER_ADDR "192.168.1.6"
#define SERVER_PORT (40000)
#define NB_THREADS (64)
#define NB_WORK_UNITS (1000000)
#define DO_CLOSED_LOOP (0)
#define BLOCKING_SENDER (0)

// Request structure
struct RpcRequestMsg {
    int id;
};

// Response structure
struct RpcResponseMsg {
    int id;
};

// A single work unit
struct WorkUnit {
    RpcRequestMsg req;
    RpcResponseMsg resp;
};

// A single client thread state
struct ClientState {
    std::thread td;
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_mr *mr;
    struct ibv_comp_channel *scomp;
    struct ibv_cq *scq;
    struct ibv_comp_channel *rcomp;
    struct ibv_cq *rcq;
    std::vector<WorkUnit> work_units;
    struct {
        uint64_t nb_reqs;
        uint64_t duration;
    } stats;
};


// Barrier to synchronize the client threads local to this machine
class Barrier {
public:
    explicit Barrier(size_t count)
        : thread_count(count), waiting(0), generation(0) {}

    void wait() {
        std::unique_lock<std::mutex> lock(mtx);
        size_t gen = generation;

        if (++waiting == thread_count) {
            // All threads have arrived, reset the counter and proceed
            waiting = 0;
            generation++;
            cv.notify_all();
        } else {
            // Wait for all threads to reach the barrier
            cv.wait(lock, [this, gen] { return gen != generation; });
        }
    }

private:
    size_t thread_count; // Number of threads to wait for
    size_t waiting;      // Number of threads currently waiting
    size_t generation;   // Barrier generation to handle multiple phases
    std::mutex mtx;
    std::condition_variable cv;
} *barrier;


void clientProcessWorkUnitsSync(ClientState *clnt,
                                std::vector<WorkUnit> &work_units) {

    int status;
    struct ibv_recv_wr rwr = {0};
    struct ibv_recv_wr *bad_rwr;
    struct ibv_send_wr swr = {0};
    struct ibv_send_wr *bad_swr;
    struct ibv_sge sge;
    struct ibv_wc wc;

    barrier->wait();

    auto start_time = std::chrono::steady_clock::now();

    for (int i = 0; i < work_units.size(); i++) {

        // Prepare the response memory
        sge.addr = (uint64_t)&work_units[i].resp;
        sge.length = sizeof(work_units[i].resp);
        sge.lkey = clnt->mr->lkey;
        rwr.wr_id = i;
        rwr.next = NULL;
        rwr.sg_list = &sge;
        rwr.num_sge = 1;
        status = ibv_post_recv(clnt->id->qp, &rwr, &bad_rwr);
        assert(!status);

        // Send the request
        sge.addr = (uint64_t)&work_units[i].req;
        sge.length = sizeof(work_units[i].req);
        sge.lkey = clnt->mr->lkey;
        swr.wr_id = i;
        swr.next = NULL;
        swr.sg_list = &sge;
        swr.num_sge = 1;
        swr.opcode = IBV_WR_SEND;
        status = ibv_post_send(clnt->id->qp, &swr, &bad_swr);
        assert(!status);

        // Wait for the request to be sent
        while (ibv_poll_cq(clnt->scq, 1, &wc) <= 0);
        assert(wc.wr_id == i);

        // Wait for the response to be received
        while (ibv_poll_cq(clnt->rcq, 1, &wc) <= 0);
        assert(wc.wr_id == i);

        // Update stats
        clnt->stats.nb_reqs++;
    }

    auto end_time = std::chrono::steady_clock::now();
    clnt->stats.duration = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - start_time).count();

    barrier->wait();
}

void clientProcessWorkUnitsAsync(ClientState *clnt,
                                 std::vector<WorkUnit> &work_units) {

    int status;
    struct ibv_recv_wr rwr = {0};
    struct ibv_recv_wr *bad_rwr;
    struct ibv_send_wr swr = {0};
    struct ibv_send_wr *bad_swr;
    struct ibv_sge rsge = {0};
    struct ibv_sge ssge = {0};
    struct ibv_wc swcs[64];
    RpcResponseMsg resps[128];
    struct ibv_mr *mr;

    mr = ibv_reg_mr(clnt->pd,
                    (void *)resps,
                    sizeof(resps),
                    IBV_ACCESS_LOCAL_WRITE |
                    IBV_ACCESS_REMOTE_READ |
                    IBV_ACCESS_REMOTE_READ);
    assert(mr);

    // Post the receive work requests
    for (int i = 0; i < 128; i++) {

        rsge.addr = (uint64_t)&resps[i];
        rsge.length = sizeof(resps[i]);
        rsge.lkey = mr->lkey;
        rwr.wr_id = i;
        rwr.next = NULL;
        rwr.sg_list = &rsge;
        rwr.num_sge = 1;
        status = ibv_post_recv(clnt->id->qp, &rwr, &bad_rwr);
        assert(!status);
    }

    // Asynchronous thread to process the responses
    std::thread async_reader([&]() {

        int status;
        struct ibv_cq *cq = NULL;
        void *cq_cxt = NULL;
        int nwcs;
        struct ibv_wc rwc[64];
        int epoll_fd;
        struct epoll_event event;
        int nproc = 0;

        // Change the blocking mode of the completion channel
        int flags = fcntl(clnt->rcomp->fd, F_GETFL);
        status = fcntl(clnt->rcomp->fd, F_SETFL, flags | O_NONBLOCK);
        assert(!status);

        // Configure epoll settings
        epoll_fd = epoll_create1(0);
        assert(epoll_fd != -1);
        event.events = EPOLLIN;
        event.data.fd = clnt->rcomp->fd;
        status = epoll_ctl(epoll_fd, EPOLL_CTL_ADD,
                           clnt->rcomp->fd, &event);

        while (true) {

            // Do a bounded blocked wait
            int nfds = epoll_wait(epoll_fd, &event, 1, 10000);
            if (!nfds) {
                continue;
            }

            // Wait for completion event (edge triggered)
            status = ibv_get_cq_event(clnt->rcomp, &cq, &cq_cxt);
            if (status) {
                continue;
            }

            // Ack the events
            ibv_ack_cq_events(cq, 1);

            // (Re)arm the completion queue
            status = ibv_req_notify_cq(cq, 0);
            assert(!status);

            // Process the completions (till there are none left)
            while (true) {

                // Get the completion
                int _nwc = ibv_poll_cq(cq, 16, rwc);
                assert(_nwc >= 0);
                if (!_nwc) {
                    break;
                }

                for (int i = 0; i < _nwc; i++) {
                    // Process the response message
                    RpcResponseMsg &resp = resps[rwc[i].wr_id];
                    work_units[resp.id].resp = resp;

                    nproc++;
                    clnt->stats.nb_reqs++;

                    // Post a new receive work request
                    rsge.addr = (uint64_t)&resps[rwc[i].wr_id];
                    rsge.length = sizeof(resps[rwc[i].wr_id]);
                    rsge.lkey = mr->lkey;
                    rwr.wr_id = rwc[i].wr_id;
                    rwr.next = NULL;
                    rwr.sg_list = &rsge;
                    rwr.num_sge = 1;
                    status = ibv_post_recv(clnt->id->qp, &rwr, &bad_rwr);
                    assert(!status);
                }
            }

            if (nproc >= work_units.size()) {
                return;
            }
        }
    });

    int active_swrs = 0;

    barrier->wait();

    auto start_time = std::chrono::steady_clock::now();

    // Main client thread generates load
    for (int i = 0; i < work_units.size(); i++) {

#if (BLOCKING_SENDER == 1)
        if (active_swrs >= 128) {

            struct ibv_cq *cq = NULL;
            void *cq_cxt = NULL;

            // Wait for completion event (edge triggered)
            status = ibv_get_cq_event(clnt->scomp, &cq, &cq_cxt);
            assert(!status);

            // Ack the events
            ibv_ack_cq_events(cq, 1);

            // (Re)arm the completion queue
            status = ibv_req_notify_cq(cq, 0);
            assert(!status);

            int _nwcs;
            while ((_nwcs = ibv_poll_cq(cq, 16, swcs)) > 0) {
                active_swrs = active_swrs - _nwcs;
            }
        }
#endif

        // Send the request
        ssge.addr = (uint64_t)&work_units[i].req;
        ssge.length = sizeof(work_units[i].req);
        ssge.lkey = clnt->mr->lkey;
        swr.wr_id = i;
        swr.next = NULL;
        swr.sg_list = &ssge;
        swr.num_sge = 1;
        swr.opcode = IBV_WR_SEND;
        status = ibv_post_send(clnt->id->qp, &swr, &bad_swr);
        assert(!status);
        active_swrs++;

#if (BLOCKING_SENDER == 0)
        while (ibv_poll_cq(clnt->scq, 1, swcs) <= 0);
#endif
    }

    // Stop the async thread
    async_reader.join();

    auto end_time = std::chrono::steady_clock::now();
    clnt->stats.duration = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - start_time).count();

    ibv_dereg_mr(mr);

    barrier->wait();
}


// Client load generation loop
void clientLoop(ClientState *clnt) {

    int status;
    struct rdma_event_channel *ec;
    struct sockaddr_in addr;
    struct rdma_cm_event *ev;
    struct ibv_device_attr dev_attr;
    struct ibv_qp_init_attr qp_init_attr;
    struct rdma_conn_param conn_param;
    std::vector<WorkUnit> work_units;

    // Create the event channel
    ec = rdma_create_event_channel();
    assert(ec);

    // Create the rdma socket
    status = rdma_create_id(ec, &clnt->id, NULL, RDMA_PS_TCP);
    assert(!status);

    // Bind to an RDMA device
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_ADDR, &addr.sin_addr);
    status = rdma_resolve_addr(clnt->id, NULL, (struct sockaddr *)&addr, 10000);
    assert(!status);

    // Wait for the address resolution
    status = rdma_get_cm_event(ec, &ev);
    assert(!status);
    assert(ev->event == RDMA_CM_EVENT_ADDR_RESOLVED);
    status = rdma_ack_cm_event(ev);
    assert(!status);

    // Get the device attributes
    status = ibv_query_device(clnt->id->verbs, &dev_attr);
    assert(!status);

    // Create a protection domain
    clnt->pd = ibv_alloc_pd(clnt->id->verbs);
    assert(clnt->pd);

    // Create the memory region
    work_units.resize(NB_WORK_UNITS);
    for (int i = 0; i < NB_WORK_UNITS; i++) {
        work_units[i].req.id = i;
        work_units[i].resp.id = 0;
    }
    clnt->mr = ibv_reg_mr(clnt->pd,
                         (void *)work_units.data(),
                         work_units.size() * sizeof(WorkUnit),
                         IBV_ACCESS_LOCAL_WRITE |
                         IBV_ACCESS_REMOTE_READ |
                         IBV_ACCESS_REMOTE_READ);
    assert(clnt->mr);

    // Create the send completion queue
    clnt->scomp = ibv_create_comp_channel(clnt->id->verbs);
    assert(clnt->scomp);
    clnt->scq = ibv_create_cq(clnt->id->verbs, dev_attr.max_cqe, NULL, clnt->scomp, 0);
    assert(clnt->scq);
    status = ibv_req_notify_cq(clnt->scq, 0);
    assert(!status);

    // Create receive send completion queue
    clnt->rcomp = ibv_create_comp_channel(clnt->id->verbs);
    assert(clnt->rcomp);
    clnt->rcq = ibv_create_cq(clnt->id->verbs, dev_attr.max_cqe, NULL, clnt->rcomp, 0);
    assert(clnt->rcq);
    status = ibv_req_notify_cq(clnt->rcq, 0);
    assert(!status);

    // Create the queue pair
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.send_cq = clnt->scq;
    qp_init_attr.recv_cq = clnt->rcq;
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 1;
    qp_init_attr.cap.max_send_wr = 128;
    qp_init_attr.cap.max_recv_wr = 128;
    qp_init_attr.cap.max_send_sge = dev_attr.max_sge;
    qp_init_attr.cap.max_recv_sge = dev_attr.max_sge;
    status = rdma_create_qp(clnt->id, clnt->pd, &qp_init_attr);
    assert(!status);

    // Resolve the route to the remote address
    status = rdma_resolve_route(clnt->id, 10000);
    assert(!status);

    // Wait for the route resolution
    status = rdma_get_cm_event(ec, &ev);
    assert(!status);
    assert(ev->event == RDMA_CM_EVENT_ROUTE_RESOLVED);
    status = rdma_ack_cm_event(ev);
    assert(!status);

    // Connect with the remote
    memset(&conn_param, 0, sizeof(conn_param));
    status = rdma_connect(clnt->id, &conn_param);

    // Wait for the connection
    status = rdma_get_cm_event(ec, &ev);
    assert(!status);
    assert(ev->event == RDMA_CM_EVENT_ESTABLISHED);
    status = rdma_ack_cm_event(ev);
    assert(!status);

    // Perform the work
#if (DO_CLOSED_LOOP == 1)
    clientProcessWorkUnitsSync(clnt, work_units);
#else
    clientProcessWorkUnitsAsync(clnt, work_units);
#endif

    // Verify the work was successfull
    for (int i = 0; i < NB_WORK_UNITS; i++) {
        assert(work_units[i].req.id == work_units[i].resp.id);
    }

    // Disconnect with the remote
    status = rdma_disconnect(clnt->id);
    assert(!status);

    // Wait for the disconnection
    status = rdma_get_cm_event(ec, &ev);
    assert(!status);
    assert(ev->event == RDMA_CM_EVENT_DISCONNECTED);
    status = rdma_ack_cm_event(ev);
    assert(!status);

    // Clean up
    rdma_destroy_qp(clnt->id);
    ibv_destroy_cq(clnt->rcq);
    ibv_destroy_comp_channel(clnt->rcomp);
    ibv_destroy_cq(clnt->scq);
    ibv_destroy_comp_channel(clnt->scomp);
    ibv_dereg_mr(clnt->mr);
    ibv_dealloc_pd(clnt->pd);
    rdma_destroy_id(clnt->id);
    rdma_destroy_event_channel(ec);
}


int main(int argc, char *argv[]) {

    barrier = new Barrier(NB_THREADS);
    std::vector<ClientState> clients(NB_THREADS);

    // Create the client threads
    for (int i = 0; i < NB_THREADS; i++) {

        clients[i].td = std::thread(clientLoop, &clients[i]);
    }

    // Wait for the client threads
    for (auto &client : clients) {
        client.td.join();
    }

    // Print the statistics
    double tot_tput;
    for (int i = 0; i < NB_THREADS; i++) {
        double clnt_tput = (double)clients[i].stats.nb_reqs / clients[i].stats.duration;
        tot_tput += clnt_tput;
    }
    std::cout << "Throughput: " << tot_tput << " Million Requests Per Second" << std::endl;

    return 0;
}
