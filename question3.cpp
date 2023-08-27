#include <iostream>
#include <map>
#include <vector>
#include <queue>
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <stdio.h>
#include <time.h>
#include <set>

using namespace std;

/*
The number of services n in the system
➢ The number of worker threads m for each service
➢ The priority level and resources assigned to each worker thread. Each worker thread
should be on a separate line and its information should be separated by spaces in the
following format: priority_level resources
➢ The type of transaction associated with each request, and the number of resources required
for that transaction. Each request should be on a separate line and its information should
be separated by spaces in the following format: transaction_type resources_required
• Your program should output the following information:
➢ The order in which requests were processed
➢ The average waiting time for requests
➢ The average turnaround time for requests
➢ The number of requests that were rejected due to lack of resources
• Additionally, your program should output the following information during periods of high traffic:
➢ The number of requests that were forced to wait due to lack of available resources
➢ The number of requests that were blocked due to the absence of any available worker
threads
*/

// struct to hold worker thread info
struct WorkerThread {
    int priority_level;
    int resources;

    WorkerThread() {}
    WorkerThread(int pl, int rs) {
        priority_level = pl;
        resources = rs;
    }
};

// struct to hold worker Request info
struct Request{
    int request_type;
    int resources;
    int burst_time;
    clock_t start_time, end_time;

    Request(int rqt, int rs) {
        request_type = rqt;
        resources = rs;
        burst_time = (rand()%8 + 2)*1000; // in ms
    }

};

int n, m; // No of services, no of worker threads per service
int blocked_due_to_resouces = 0; // No of requets blocked due to resouces constraint
int blocked_due_to_wt = 0; // No of requets blocked due to resouces constraint

vector<bool> allTaskDone; // Flag variable for denoting all the requests are done for a service
vector<mutex> task_Done_locks; // lock for above flags

vector<queue<Request>> requests; // Requests for each services   
vector<vector<WorkerThread>> worker_threads; // worker threads for each services

vector<vector<queue<Request>>> queues; //queus for each worker thread (n*m no of queues)
vector<vector<mutex>> locksForQ; // Lock for above queues

vector<Request> executed_requests; // completed requests
mutex erl; // mutex for  executed_requests
vector<Request> wt_blocked_requests; // blocked request by all wt (not possible to process)
set<pair<int, int>> temp_blocked_requests; // temp blocked requests due to resources



clock_t START; // Starting time  = 0

// Thread spawned by WorkerThread to complete the request.
void do_work(int pid, int wtid, Request rq) {
    // sleep for burst time
    this_thread::sleep_for(chrono::microseconds(rq.burst_time));
    
    // add resouces back to workerThread
    worker_threads[pid][wtid].resources += rq.resources;
    
    //save the end time of request 
    rq.end_time = clock();

    // push the executed request
    erl.lock();
    executed_requests.push_back(rq);
    erl.unlock();
    
    printf("Request done = {%d , %d} ======> wt[%d][%d]\n", rq.request_type, rq.resources, pid, wtid);
}

// WorkerThread spawned by service
void run_worker_thread(int pid, int wtid) {
    
    vector<thread> children;
    while(true) {
        // lock
        // check value of task done flag
        task_Done_locks[pid].lock();
        bool taskDone =  allTaskDone[pid];
        task_Done_locks[pid].unlock();

        locksForQ[pid][wtid].lock();
        
        // id both are true then all proessing is complete so exit
        if(taskDone && queues[pid][wtid].empty()){ 
            locksForQ[pid][wtid].unlock();
            break;
        }

        // if all tasks(requests) is not completed but queue is empty then check again(i.e wait for request to come).
        if(queues[pid][wtid].empty()) {
            locksForQ[pid][wtid].unlock();
            continue;
        }
        
        Request rq = queues[pid][wtid].front();
        queues[pid][wtid].pop();
        // unlock
        locksForQ[pid][wtid].unlock();

        // Create thread to execute request
        children.push_back(thread(do_work, pid, wtid, rq));
    }

    // join(wait) all the children threads
    for(int i = 0; i < children.size(); i++) {
        children[i].join();
    }
}


// Service thread created by main
void do_service(int pid) {
    int maxResouce = 0;
    vector<thread> threads;
    for(int i=0; i < m; i++) {
        // creating m workerThreads
        threads.push_back(thread(run_worker_thread, pid, i));
        // checking for max resouces violation
        maxResouce = max(maxResouce, (int)worker_threads[pid][i].resources);
    }
    
    while(!requests[pid].empty()) {
        Request rq = requests[pid].front();
        requests[pid].pop();
        
        // cannot get enough resouces, so reject
        if(rq.resources > maxResouce) { 
            blocked_due_to_wt++;
            printf("\n------------------------------\n");
            printf("| Request Blocked = {%2d, %2d} |\n", rq.request_type, rq.resources);
            printf("------------------------------\n\n");
            erl.lock();
            wt_blocked_requests.push_back(rq);
            erl.unlock();
            continue;
        }
        
        // Flag to check if request is satisfied or not
        int flag = 0;
        for(int i = m-1; i >= 0; i--) {
            locksForQ[pid][i].lock(); //lock
            if(worker_threads[pid][i].resources >= rq.resources){
                // decreasing resouces of workerThread
                worker_threads[pid][i].resources -= rq.resources;    
                
                // adding start time for request.
                rq.start_time = clock();
                queues[pid][i].push(rq);
                
                // unlock
                locksForQ[pid][i].unlock(); 
                flag = 1;
                printf("th[%d] ----> Request pushed = {%d, %d}\n", pid, rq.request_type, rq.resources);
                break;
            }  
            // unlock
            locksForQ[pid][i].unlock(); 
        }
        // request not alooted, try again.
        if(flag == 0) {
            // increase block count
            blocked_due_to_resouces++;
            temp_blocked_requests.insert(make_pair(rq.request_type, rq.resources));
            // locksForQ[pid][m].lock();
            requests[pid].push(rq);
            // locksForQ[pid][m].unlock();
        }
        // this_thread::sleep_for(500ms);
    }
    
    // wating to complete all threads
    this_thread::sleep_for(5000ms);

    // making all tasks(requests) done flag true.
    task_Done_locks[pid].lock();
    allTaskDone[pid] = true;
    task_Done_locks[pid].unlock();

    // Join the m workerThreads
    for(int i=0; i < m; i++) {
        threads[i].join();
    }
}

// print all the final statistics of processing all requets.
void printStatistics() {
    printf("\n\nProcessing Order : \n");
    printf("---------------------------------------------------------------------------------\n");
    printf("| No | Request{ty,rs} |  start  |   end   |  burst  |  turn around  |  waiting  |\n");
    printf("---------------------------------------------------------------------------------\n");

    double avg_wating_time = 0, avg_turn_around_time = 0;
    int i = 0;
    for(auto rq : executed_requests) {
        i++;
        double st = double(rq.start_time-START) / double(CLOCKS_PER_SEC);
        double et = double(rq.end_time-START) / double(CLOCKS_PER_SEC);
        double bt = double(rq.burst_time)/1000000;
        double tt = double(rq.end_time-rq.start_time) / double(CLOCKS_PER_SEC);
        double wt = tt-bt;
        avg_wating_time += wt;
        avg_turn_around_time += tt;
        printf("| %2d |     {%2d, %2d}   | %7.5f | %7.5f | %7.5f |       %7.5f |  %7.5f  |\n", i, rq.request_type, rq.resources, st, et, bt, tt, wt);
    }       
    printf("---------------------------------------------------------------------------------\n\n");

    printf("Blocked(work Thread) Requests : \n");
    printf("-----------------------\n");
    printf("| No | Request{ty,rs} |\n");
    printf("-----------------------\n");
    i = 0;
    for(auto rq : wt_blocked_requests) {
        i++;
        printf("| %2d |     {%2d, %2d}   |\n", i, rq.request_type, rq.resources);
    }
    printf("-----------------------\n\n");


    printf("Temp (Resource) Blocked Requests : \n");
    printf("-----------------------\n");
    printf("| No | Request{ty,rs} |\n");
    printf("-----------------------\n");
    i = 0;
    for(auto rq : temp_blocked_requests) {
        i++;
        printf("| %2d |     {%2d, %2d}   |\n", i, rq.first, rq.second);
    }
    printf("-----------------------\n\n");


    printf("-----------------------------------------------------------\n");
    printf("| Average waiting time :     %7.5f                      |\n", avg_wating_time/executed_requests.size());
    printf("| Average turn around time : %7.5f                      |\n", avg_turn_around_time/executed_requests.size());
    printf("| No of times Request blocked due to resource :   %7d |\n", blocked_due_to_resouces);
    printf("| Request blocked due to worker thread :          %7d |\n", blocked_due_to_wt);
    printf("-----------------------------------------------------------\n");

}

int main(int argc, char const *argv[])
{
    cin >> n >> m; // NO of services, NO of worker threads
    // worker_threads.resize(n);
    queues.resize(n);
    task_Done_locks = vector<mutex>(n);
    allTaskDone.resize(n);

    // making n*m worker_threads
    for (int i = 0; i < n; i++) {
        worker_threads.push_back(vector<WorkerThread>(m));
    }

    // Taking input
    for (int i = 0; i < n; i++) {
        allTaskDone[i] = false;
        locksForQ.push_back(vector<mutex>(m));
        // task_Done_locks.push_back(new mutex());
        queues[i].resize(m);
        for (int j = 0; j < m; j++) {
            int pt, rs;
            cin >> pt >> rs;
            // worker_threads[i].push_back(WorkerThread(pt, rs));
            worker_threads[i][pt].priority_level = pt;
            worker_threads[i][pt].resources = rs;

            // locksForQ[i].push_back(mutex());
        }   
    }

    // cout << "n = " << locksForQ.size() << ", m = " << locksForQ[2].size() << endl;
    // printf("n = %d, m = %d\n",locksForQ.size(), locksForQ[2].size());

    // no of requests
    int k;
    cin >> k;
    requests.resize(n);
    
    // input of requests
    for (int i = 0; i < k; i++) {
        int ty, rs;
        cin >> ty >> rs;
        requests[ty].push(Request(ty, rs));
    }
    
    // start of all requests.
    START = clock();    

    // Creating n services
    vector<thread> services;
    for(int i = 0; i < n; i++) {
        services.push_back(thread(do_service, i));
    } 
    // cout << "input Done" << endl;
    printf("Input Done\n");

    // join all service threads to finish
    for(int i = 0; i < n; i++) {
        services[i].join();
    } 

    // print all the statistics for the requets.
    printStatistics();

    printf("Finished!\n");
    
    return 0;
}
