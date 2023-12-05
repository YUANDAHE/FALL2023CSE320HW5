
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/select.h>
#include <semaphore.h>

#include "csapp.h"
#include "debug.h"
#include "client_registry.h"
#include "transaction.h"
#include "store.h"
#include "protocol.h"

pthread_mutex_t client_mutex;

struct client_registry 
{
    fd_set clientfd_set;
    int clients;
    sem_t wait_sem;
};

CLIENT_REGISTRY *creg_init()
{
    CLIENT_REGISTRY *cr = malloc(sizeof(CLIENT_REGISTRY));
    memset(cr, 0x00, sizeof(CLIENT_REGISTRY));
    
    pthread_mutex_init(&client_mutex, NULL);
    Sem_init(&cr->wait_sem,0, 0);
    
    return cr;
}

void creg_fini(CLIENT_REGISTRY *cr)
{
    free(cr);
    
    return;
}

int creg_register(CLIENT_REGISTRY *cr, int fd)
{
    pthread_mutex_lock(&client_mutex);
    FD_SET(fd, &cr->clientfd_set);
    cr->clients++;
    pthread_mutex_unlock(&client_mutex);
    
    return 0;
}

int creg_unregister(CLIENT_REGISTRY *cr, int fd)
{
    int ret = -1;
    
    pthread_mutex_lock(&client_mutex);
    if (FD_ISSET(fd, &cr->clientfd_set)) 
    {
        ret = 0;
        FD_CLR(fd, &(cr->clientfd_set));
        cr->clients--;
    }
    pthread_mutex_unlock(&client_mutex);

    if (cr->clients <= 0) 
    {
         V(&cr->wait_sem); 
    }

    return ret;
}

void creg_wait_for_empty(CLIENT_REGISTRY *cr)
{
    pthread_mutex_lock(&client_mutex);
    while (cr->clients > 0) 
    {
        pthread_mutex_unlock(&client_mutex);
        
        P(&cr->wait_sem);
        pthread_mutex_lock(&client_mutex);
    }
    pthread_mutex_unlock(&client_mutex);
}

void creg_shutdown_all(CLIENT_REGISTRY *cr)
{
    int i;

    pthread_mutex_lock(&client_mutex);
    
    for (i = 0; i < FD_SETSIZE; i++) 
    {
        if (FD_ISSET(i, &cr->clientfd_set)) 
        {
            shutdown(i, SHUT_RD);
        }
    }

    pthread_mutex_unlock(&client_mutex);
}

