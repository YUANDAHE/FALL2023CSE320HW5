
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "csapp.h"
#include "debug.h"
#include "client_registry.h"
#include "transaction.h"
#include "store.h"
#include "protocol.h"

static void terminate(int status);
extern CLIENT_REGISTRY *cregistry;

static int fd;
extern void *xacto_client_service(void *client_arg);

static void handler_sigup(int sig) 
{
    terminate(0);
    close(fd);
    
    return;
}

int main(int argc, char* argv[])
{
    // Option processing should be performed here.
    // Option '-p <port>' is required in order to specify the port number
    // on which the server should listen.

    // Perform required initializations of the client_registry,
    // transaction manager, and object store.
    int client_fd;
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    pthread_t tid;
    
    if (argc != 3) 
    {
        fprintf(stderr, "must -p <port>\n");
        return -1;
    }
    
    if (strcmp(argv[1], "-p") != 0) 
    {
        fprintf(stderr, "must -p <port>\n");
        return -1;
    }
    
    if ((fd = open_listenfd(argv[2])) < 0) 
    {
		fprintf(stderr, "open_listenfd %s error\n", argv[2]);
		return -1;
	}

    cregistry = creg_init();
    trans_init();
    store_init();
    
    Signal(SIGHUP, handler_sigup);

    // TODO: Set up the server socket and enter a loop to accept connections
    // on this socket.  For each connection, a thread should be started to
    // run function xacto_client_service().  In addition, you should install
    // a SIGHUP handler, so that receipt of SIGHUP will perform a clean
    // shutdown of the server.
    while (1) 
    {
        client_fd = Accept(fd, (SA *)&client_addr, &client_len);
        if (client_fd <= 0) 
        {
            perror("accept");
            break;
        }
        
        creg_register(cregistry, client_fd);
        Pthread_create(&tid, NULL, xacto_client_service, (void *)(unsigned long)client_fd);
    }

    terminate(EXIT_FAILURE);
    close(fd);
    return 0;
}

/*
 * Function called to cleanly shut down the server.
 */
static void terminate(int status) 
{
    // Shutdown all client connections.
    // This will trigger the eventual termination of service threads.
    creg_shutdown_all(cregistry);
    
    debug("Waiting for service threads to terminate...");
    creg_wait_for_empty(cregistry);
    debug("All service threads terminated.");

    // Finalize modules.
    creg_fini(cregistry);
    trans_fini();
    store_fini();

    debug("Xacto server terminating");
    exit(status);
}

