#include <cassert>
#include <algorithm>
#include <string>
#include <iostream>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <sched.h>
#include <unistd.h>
#include <pthread.h>
#include <thread>
#include <chrono>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <getopt.h>

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>


#define CACHE_LINE_SIZE (64u)
#define SERVER_ADDR "192.168.1.6"
#define SERVER_PORT (40000)
#define LISTEN_QUEUE_LEN (1024)
#define NB_POLLERS (2)
#define POOL_SIZE (500000)
#define RX_BURST_SIZE (64u)
#define TX_BURST_SIZE (64u)


// Request structure
struct RpcRequestMsg {
    int id;
};

// Response structure
struct RpcResponseMsg {
    int id;
};


// A single client request state
struct alignas(CACHE_LINE_SIZE) RequestState {
    struct ibv_qp *qp;
    RpcRequestMsg req;
    RpcResponseMsg resp;
    RequestState *prev;         // Link in lists
    RequestState *next;         // Link in lists
};

// List of request states
class RequestStateList {
private:
    RequestState *head;
    RequestState *tail;

public:
    RequestStateList() {
        head = NULL;
        tail = NULL;
    }

    bool empty() {

        return (!head);
    }

    void push_back(RequestState *req_state) {

        if (!tail) {
            req_state->prev = NULL;
            req_state->next = NULL;
            head = tail = req_state;
        } else {
            tail->next = req_state;
            req_state->prev = tail;
            req_state->next = NULL;
            tail = req_state;
        }
    }

    void push_front(RequestState *req_state) {

        if (!head) {
            req_state->prev = NULL;
            req_state->next = NULL;
            head = tail = req_state;
        } else {
            req_state->prev = NULL;
            req_state->next = head;
            head->prev = req_state;
            head = req_state;
        }
    }

    RequestState *pop_back() {

        if (!head) {
            return NULL;
        }

        RequestState *req_state = tail;

        tail = tail->prev;
        if (!tail) {
            head = NULL;
        } else {
            tail->next = NULL;
        }

        return req_state;
    }

    RequestState *pop_front() {

        if (!head) {
            return NULL;
        }

        RequestState *req_state = head;

        head = head->next;
        if (!head) {
            tail = NULL;
        } else {
            head->prev = NULL;
        }

        return req_state;
    }
};

// Request state pool
class RequestStatePool {
private:
    std::vector<RequestState> pool;
    RequestStateList free_list;
    int pool_sz;

public:
    void init(int pool_size) {

        pool_sz = pool_size;

        // Create a request state pool
        pool.resize(pool_size);

        // Initialize the free list
        for (int i = 0; i < pool_size; i++) {
            free_list.push_back(&pool[i]);
        }
    }

    RequestState *get() {

        return free_list.pop_front();
    }

    void put(RequestState *req_state) {

        free_list.push_back(req_state);
    }

    friend struct ibv_mr *ibv_reg_pool(const RequestStatePool &pool, struct ibv_pd *pd);
};

// Helper to register the request state pool to the given protection domain
struct ibv_mr *ibv_reg_pool(const RequestStatePool &pool, struct ibv_pd *pd) {

    return ibv_reg_mr(pd,
                      (void *)pool.pool.data(),
                      pool.pool.size() * sizeof(RequestState),
                      IBV_ACCESS_LOCAL_WRITE |
                      IBV_ACCESS_REMOTE_READ |
                      IBV_ACCESS_REMOTE_READ);
}


// A single poller thread state
struct PollerState {
    std::thread td;             // Thread handle
    struct rdma_cm_id *id;      // RDMA CM handle
    struct ibv_pd *pd;          // Protection domain
    struct ibv_mr *mr;          // Memory region
    struct ibv_srq *srq;        // Shared receive queue
    struct ibv_cq *scq;         // Send completion queue
    struct ibv_cq *rcq;         // Receive completion queue
    std::unordered_map<uint32_t, struct ibv_qp *> qpnum_to_qp; // Queue pair number to queue pair structure map
    RequestStatePool pool;      // Pool of client request states
    RequestStateList work_que;  // Queue of request states simulating workers
    std::mutex mtx;             // Mutex used for any shared operations
};


void pollerProcessRxBurst(PollerState *poller) {

    int status;
    struct ibv_recv_wr rwr = {0};
    struct ibv_recv_wr *bad_rwr;
    struct ibv_sge sge = {0};
    RequestState *req_st;
    int nwcs;
    struct ibv_wc wcs[RX_BURST_SIZE];

    // Check if we have received any requests
    nwcs = ibv_poll_cq(poller->rcq, RX_BURST_SIZE, wcs);
    assert(nwcs >= 0);

    // No requests received
    if (!nwcs) {
        return;
    }

    for (int i = 0; i < nwcs; i++) {

        // Get the associated request state
        req_st = (RequestState *)wcs[i].wr_id;

        // Link the queue pair
        poller->mtx.lock();
        req_st->qp = poller->qpnum_to_qp[wcs[i].qp_num];
        poller->mtx.unlock();

        // Pass the request to the worker
        poller->work_que.push_back(req_st);

        // Post a new receive work request
        req_st = poller->pool.get();
        if (!req_st) {
            continue;
        }
        sge.addr = (uint64_t)&req_st->req;
        sge.length = sizeof(req_st->req);
        sge.lkey = poller->mr->lkey;
        rwr.wr_id = (uint64_t)req_st;
        rwr.next = NULL;
        rwr.sg_list = &sge;
        rwr.num_sge = 1;
        status = ibv_post_srq_recv(poller->srq, &rwr, &bad_rwr);
        assert(!status);
    }
}

void pollerProcessTxBurst(PollerState *poller, int &outstanding_tx) {

    int status;
    struct ibv_send_wr swr = {0};
    struct ibv_send_wr *bad_swr;
    struct ibv_sge sge = {0};
    RequestState *req_st;
    int nwcs;
    struct ibv_wc wcs[TX_BURST_SIZE];

    for (int i = 0; (i < TX_BURST_SIZE) && (outstanding_tx < 2048); i++) {

        // Get the processed request from the worker
        if (poller->work_que.empty()) {
            break;
        }
        req_st = poller->work_que.pop_front();

        // Update the response message (should be ideally done in the worker)
        req_st->resp.id = req_st->req.id;

        // Send the response
        sge.addr = (uint64_t)&req_st->resp;
        sge.length = sizeof(req_st->resp);
        sge.lkey = poller->mr->lkey;
        swr.wr_id = (uint64_t)req_st;
        swr.next = NULL;
        swr.sg_list = &sge;
        swr.num_sge = 1;
        swr.opcode = IBV_WR_SEND;
        status = ibv_post_send(req_st->qp, &swr, &bad_swr);
        assert(!status);

        // Keep track of outstanding send WRs
        outstanding_tx++;
    }

    // Process the send completions
    nwcs = ibv_poll_cq(poller->scq, TX_BURST_SIZE, wcs);
    assert(nwcs >= 0);

    // No requests received
    if (!nwcs) {
        return;
    }

    for (int i = 0; i < nwcs; i++) {

        // Get the associated request state
        req_st = (RequestState *)wcs[i].wr_id;

        // Free the request state to the pool
        poller->pool.put(req_st);

        // Keep track of outstanding send WRs
        outstanding_tx--;
    }
}


// Main poller datapath loop
void pollerLoop(PollerState *poller) {

    int status;
    struct ibv_recv_wr rwr;
    struct ibv_recv_wr *bad_rwr;
    struct ibv_sge sge;
    RequestState *req_st;
    int outstanding_tx = 0;

    // Prepare the shared receive queue
    while (true) {

        // Get a free request state
        req_st = poller->pool.get();
        if (!req_st) {
            break;
        }

        // Post a receive work request
        sge.addr = (uint64_t)&req_st->req;
        sge.length = sizeof(req_st->req);
        sge.lkey = poller->mr->lkey;
        rwr.wr_id = (uint64_t)req_st;
        rwr.next = NULL;
        rwr.sg_list = &sge;
        rwr.num_sge = 1;
        status = ibv_post_srq_recv(poller->srq, &rwr, &bad_rwr);
        if (status) {
            break;
        }
    }

    // Main datapath loop
    while (true) {

        pollerProcessRxBurst(poller);
        pollerProcessTxBurst(poller, outstanding_tx);
    }
}


int main(int argc, char *argv[]) {

    int status;
    struct rdma_event_channel *ec;
    struct rdma_cm_id *serv_id;
    struct rdma_cm_event *ev;
    struct sockaddr_in addr;
    struct ibv_device_attr dev_attr;
    struct ibv_srq_init_attr srq_attr;
    struct ibv_qp_init_attr qp_init_attr;
    PollerState pollers[NB_POLLERS];
    int next_poller = 0;
    std::unordered_map<uint32_t, uint32_t> qpnum_to_poller;

    // Create the event channel
    ec = rdma_create_event_channel();
    assert(ec);

    // Create the rdma socket
    status = rdma_create_id(ec, &serv_id, NULL, RDMA_PS_TCP);
    assert(!status);

    // Bind the server to an address
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_ADDR, &addr.sin_addr);
    status = rdma_bind_addr(serv_id, (struct sockaddr *)&addr);
    assert(!status);

    // Get the device attributes
    status = ibv_query_device(serv_id->verbs, &dev_attr);
    assert(!status);

    // Create the poller threads and the per-poller resources
    for (int i = 0; i < NB_POLLERS; i++) {

        pollers[i].id = serv_id;

        // Create the protection domain
        pollers[i].pd = ibv_alloc_pd(serv_id->verbs);
        assert(pollers[i].pd);

        // Register a memory region for the memory pool
        pollers[i].pool.init(POOL_SIZE);
        pollers[i].mr = ibv_reg_pool(pollers[i].pool, pollers[i].pd);
        assert(pollers[i].mr);

        // Create a shared receive queue
        memset(&srq_attr, 0, sizeof(srq_attr));
        srq_attr.attr.max_wr = dev_attr.max_srq_wr;
        srq_attr.attr.max_sge = dev_attr.max_srq_sge;
        pollers[i].srq = ibv_create_srq(pollers[i].pd, &srq_attr);
        assert(pollers[i].srq);

        // Create the send completion queue
        pollers[i].scq = ibv_create_cq(serv_id->verbs, dev_attr.max_cqe,
                                       NULL, NULL, 0);
        assert(pollers[i].scq);

        // Create the receive completion queue
        pollers[i].rcq = ibv_create_cq(serv_id->verbs, dev_attr.max_cqe,
                                       NULL, NULL, 0);
        assert(pollers[i].rcq);

        // Start the poller thread
        pollers[i].td = std::thread(pollerLoop, &pollers[i]);
    }

    // Start accepting the client connections
    status = rdma_listen(serv_id, LISTEN_QUEUE_LEN);
    assert(!status);

    // Main thread loop
    while (true) {

        // Wait for an event
        status = rdma_get_cm_event(ec, &ev);
        assert(!status);

        switch (ev->event) {

        case RDMA_CM_EVENT_CONNECT_REQUEST:
        {
            std::cout << "Received a connection request\n";

            PollerState &poller = pollers[next_poller];

            // Create the queue pair
            memset(&qp_init_attr, 0, sizeof(qp_init_attr));
            qp_init_attr.send_cq = poller.scq;
            qp_init_attr.recv_cq = poller.rcq;
            qp_init_attr.srq = poller.srq;
            qp_init_attr.qp_type = IBV_QPT_RC;
            qp_init_attr.sq_sig_all = 1;
            qp_init_attr.cap.max_send_wr = 2048;
            qp_init_attr.cap.max_recv_wr = 2048;
            qp_init_attr.cap.max_send_sge = dev_attr.max_sge;
            qp_init_attr.cap.max_recv_sge = dev_attr.max_sge;
            status = rdma_create_qp(ev->id, poller.pd, &qp_init_attr);
            assert(!status);

            // Accept the client connection
            status = rdma_accept(ev->id, &ev->param.conn);
            assert(!status);

            // Add the queue pair to the pollers set of queue pairs
            poller.mtx.lock();
            poller.qpnum_to_qp[ev->id->qp->qp_num] = ev->id->qp;
            poller.mtx.unlock();

            // Remember which poller was assigned to this queue pair
            qpnum_to_poller[ev->id->qp->qp_num] = next_poller;

            next_poller = (next_poller + 1) % NB_POLLERS;

            // Ack the event
            status = rdma_ack_cm_event(ev);
            assert(!status);
        }
            break;

        case RDMA_CM_EVENT_ESTABLISHED:
        {
            // Ack the event
            status = rdma_ack_cm_event(ev);
            assert(!status);
        }
            break;

        case RDMA_CM_EVENT_DISCONNECTED:
        {
            std::cout << "Received a connection shutdown\n";

            // Tear down the connection
            status = rdma_disconnect(ev->id);

            // Ack the event
            status = rdma_ack_cm_event(ev);
            assert(!status);
        }
            break;

        case RDMA_CM_EVENT_TIMEWAIT_EXIT:
        {
            // Remove the queue pair from the pollers set of queue pairs
            PollerState &poller = pollers[qpnum_to_poller[ev->id->qp->qp_num]];
            poller.mtx.lock();
            poller.qpnum_to_qp.erase(ev->id->qp->qp_num);
            poller.mtx.unlock();

            // Destroy the queue pair created for this client
            rdma_destroy_qp(ev->id);

            // Get the id before acking
            rdma_cm_id *id = ev->id;

            // Ack the event (Need to ack before destroying the id)
            status = rdma_ack_cm_event(ev);
            assert(!status);

            // Destroy the queue pair created for this client
            rdma_destroy_id(id);
        }
            break;

        default:
        {
            std::cerr << "Received RDMA CM event: " << ev->event << std::endl;

            // We do not expect any other error codes
            assert(false);
        }
            break;
        }
    }

    // Wait for the pollers to exit
    for (int i = 0; i < NB_POLLERS; i++) {
        pollers[i].td.join();
    }

    // Clean up the per-poller resources
    for (int i = 0; i < NB_POLLERS; i++) {

        ibv_destroy_cq(pollers[i].rcq);
        ibv_destroy_cq(pollers[i].scq);
        ibv_destroy_srq(pollers[i].srq);
        ibv_dereg_mr(pollers[i].mr);
        ibv_dealloc_pd(pollers[i].pd);
    }

    // Close the rdma socket
    rdma_destroy_id(serv_id);
    rdma_destroy_event_channel(ec);

    return 0;
}
