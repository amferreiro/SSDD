#include "map.h"
#include "comun.h"
#include "kaska.h"
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stdbool.h>

bool conectado=false;
bool existe=false;
int s;
int res;
map *suscripciones;
map *recorrido_poll;
map_position *pos;




static int init_socket_client(const char *host_server, const char * port) {
    struct addrinfo *res;
    int socket_innt;
    // socket stream para Internet: TCP
    if ((socket_innt=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        perror("error creando socket");
        return -1;
    }

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;    /* solo IPv4 */
    hints.ai_socktype = SOCK_STREAM;
    
    // obtiene la dirección TCP remota
    if (getaddrinfo(host_server, port, &hints, &res)!=0) {
        perror("error en getaddrinfo");
        close(s);
        return -1;
    }
    // realiza la conexión
    if (connect(socket_innt, res->ai_addr, res->ai_addrlen) < 0) {
        perror("error en connect");
        close(socket_innt);
        return -1;
    }
    freeaddrinfo(res);
    return socket_innt;
}



// Crea el tema especificado.
// Devuelve 0 si OK y un valor negativo en caso de error.
int create_topic(char *topic){
    struct iovec iov[3]; // hay que enviar 5 elementos
    int nelem = 0;

    int entero_net = htonl(0);
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    int longitud_str = strlen(topic);
    int longitud_str_net = htonl(longitud_str);
    iov[nelem].iov_base=&longitud_str_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic; 
    iov[nelem++].iov_len=longitud_str;
    
    if(!conectado){
        if((s=init_socket_client(getenv("BROKER_HOST"),getenv("BROKER_PORT")))<0)
            return -1;
        conectado=true;
    }
    //Envío
    if (writev(s, iov, 3)<0){
        perror("Error en envío create_topic");
        conectado=false;
        close(s);
        return -1;
    }
	//Respuesta
	if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        perror("Error en recivir create_topic");
        close(s);
        return -1;
    }
    return ntohl(res);
}
// Devuelve cuántos temas existen en el sistema y un valor negativo
// en caso de error.
int ntopics(void) {
    struct iovec iov[1]; // hay que enviar 5 elementos
    int nelem = 0;

    int entero_net = htonl(1);
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    if(!conectado){
        if((s=init_socket_client(getenv("BROKER_HOST"),getenv("BROKER_PORT")))<0)
            return -1;
        conectado=true;
    }
    //Envío
    if (writev(s, iov, 1)<0){
        perror("Error en envío n_topics");
        conectado=false;
        close(s);
        return -1;
    }
	//Respuesta
	if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        perror("Error en recivir n_topics");
        close(s);
        return -1;
    }
    return ntohl(res);
}

// SEGUNDA FASE: PRODUCIR/PUBLICAR

// Envía el mensaje al tema especificado; nótese la necesidad
// de indicar el tamaño ya que puede tener un contenido de tipo binario.
// Devuelve el offset si OK y un valor negativo en caso de error.
int send_msg(char *topic, int msg_size, void *msg) {
    struct iovec iov[5];
    int nelem = 0;

    int entero_net = htonl(2);
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    int longitud_str = strlen(topic);
    int longitud_str_net = htonl(longitud_str);
    iov[nelem].iov_base=&longitud_str_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic; 
    iov[nelem++].iov_len=longitud_str;

    int longitud_arr_net = htonl(msg_size);
    iov[nelem].iov_base=&longitud_arr_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=msg; // no se usa & porque ya es un puntero
    iov[nelem++].iov_len=msg_size;

    if(!conectado){
        if((s=init_socket_client(getenv("BROKER_HOST"),getenv("BROKER_PORT")))<0)
            return -1;
        conectado=true;
    }
    //Envío
    if (writev(s, iov, 5)<0){
        perror("Error en envío send_msg");
        conectado=false;
        close(s);
        return -1;
    }
	//Respuesta
	if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        perror("Error en recivir send_msg");
        close(s);
        return -1;
    }
    return ntohl(res);
}
// Devuelve la longitud del mensaje almacenado en ese offset del tema indicado
// y un valor negativo en caso de error.
int msg_length(char *topic, int offset) {
    struct iovec iov[4]; // hay que enviar 5 elementos
    int nelem = 0;

    int entero_net = htonl(3);
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    int longitud_str = strlen(topic);
    int longitud_str_net = htonl(longitud_str);
    iov[nelem].iov_base=&longitud_str_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic; 
    iov[nelem++].iov_len=longitud_str;

    int offset_net = htonl(offset);
    iov[nelem].iov_base=&offset_net;
    iov[nelem++].iov_len=sizeof(int);

    if(!conectado){
        if((s=init_socket_client(getenv("BROKER_HOST"),getenv("BROKER_PORT")))<0)
            return -1;
        conectado=true;
    }
    //Envío
    if (writev(s, iov, 4)<0){
        perror("Error en envío msg_length");
        conectado=false;
        close(s);
        return -1;
    }
	//Respuesta
	if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        perror("Error en recivir msg_length");
        close(s);
        return -1;
    }
    
    return ntohl(res);
}

// Obtiene el último offset asociado a un tema en el broker, que corresponde
// al del último mensaje enviado más uno y, dado que los mensajes se
// numeran desde 0, coincide con el número de mensajes asociados a ese tema.
// Devuelve ese offset si OK y un valor negativo en caso de error.
int end_offset(char *topic) {
    struct iovec iov[3]; // hay que enviar 5 elementos
    int nelem = 0;

    int entero_net = htonl(4);
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    int longitud_str = strlen(topic);
    int longitud_str_net = htonl(longitud_str);
    iov[nelem].iov_base=&longitud_str_net;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic; 
    iov[nelem++].iov_len=longitud_str;

    if(!conectado){
        if((s=init_socket_client(getenv("BROKER_HOST"),getenv("BROKER_PORT")))<0)
            return -1;
        conectado=true;
    }
    //Envío
    if (writev(s, iov, 3)<0){
        perror("Error en envío end_offset");
        conectado=false;
        close(s);
        return -1;
    }
	//Respuesta
	if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        perror("Error en recivir end_offset");
        close(s);
        return -1;
    }
    return ntohl(res);
}

// TERCERA FASE: SUBSCRIPCIÓN
// Se suscribe al conjunto de temas recibidos. No permite suscripción
// incremental: hay que especificar todos los temas de una vez.
// Si un tema no existe o está repetido en la lista simplemente se ignora.
// Devuelve el número de temas a los que realmente se ha suscrito
// y un valor negativo solo si ya estaba suscrito a algún tema.
int subscribe(int ntopics, char **topics) {
    int *offset;
    int suscritos=0;
    if(existe)
        return -1;

    offset = malloc(ntopics*sizeof(int));
    suscripciones=map_create(key_string,0);
    recorrido_poll=map_create(key_string,0);
    existe = true;
    printf("Ntopics: %d\n",ntopics);
    for(int i=0;i<ntopics;i++){
        //Busco el offset del tema
        offset[i] = end_offset(topics[i]);
        if(offset[i]>=0){
            if(map_put(suscripciones, strdup(topics[i]), &offset[i])==0){
                printf("Offset: %d\n",offset[i]);
                map_put(recorrido_poll, strdup(topics[i]), &offset[i]);
                suscritos++;
            }
        }
    }
    pos = map_alloc_position(recorrido_poll);
    return suscritos;
}




// Se da de baja de todos los temas suscritos.
// Devuelve 0 si OK y un valor negativo si no había suscripciones activas.
int unsubscribe(void) {
     map_free_position(pos);
    //POLL : Libera  map_position con map_free_position (map_pos)
    if (suscripciones == NULL)
        return -1;
    if(map_destroy(suscripciones,NULL)==0){
        map_destroy(recorrido_poll,NULL);
        existe = false;
        return 0;
    }
    else return -1;
}

// Devuelve el offset del cliente para ese tema y un número negativo en
// caso de error.
int position(char *topic) {
    int *offset = malloc(sizeof(int));
    int err;
    printf("Tema: %s\n",topic);
    offset = map_get(suscripciones,topic,&err);
    if(err>=0){
        printf("offset: %d\n",*offset);
    }
    if(err<0) 
        return -1;
    return *offset;
}


// Modifica el offset del cliente para ese tema.
// Devuelve 0 si OK y un número negativo en caso de error.
int seek(char *topic, int offset) {
    int err;
    int *data = malloc(sizeof(int));
    *data = offset;
    map_get(suscripciones,topic,&err);
    if(err<0){
        printf("Error, tema no existe en mapa%s\n",topic);
        return -1;
    }
    map_remove_entry(suscripciones,topic,NULL);
    map_put(suscripciones,strdup(topic),data);
    map_remove_entry(recorrido_poll,topic,NULL);
    map_put(recorrido_poll,strdup(topic),data);
    printf("Offset actualizado\n");
    return 0;
}



// CUARTA FASE: LEER MENSAJES

// Obtiene el siguiente mensaje destinado a este cliente; los dos parámetros
// son de salida.
// Devuelve el tamaño del mensaje (0 si no había mensaje)
// y un número negativo en caso de error.

//Buscar manera de iterar según el offset local
int poll(char **topic, void **msg) {
    bool encontrado = false;
    int longitud=0; 
    char *clave;
    int *valor=malloc(sizeof(int));
    unsigned char* mensaje;
    map_iter *it;
    it = map_iter_init(recorrido_poll, pos);
    while(map_iter_has_next(it) && !encontrado){
        map_iter_value(it, (void *)&clave, (void *)&valor);

        struct iovec iov[4];
        int nelem = 0;

        int entero_net = htonl(5);
        iov[nelem].iov_base=&entero_net;
        iov[nelem++].iov_len=sizeof(int);

        int longitud_str = strlen(clave);
        int longitud_str_net = htonl(longitud_str);
        iov[nelem].iov_base=&longitud_str_net;
        iov[nelem++].iov_len=sizeof(int);
        iov[nelem].iov_base=clave; 
        iov[nelem++].iov_len=longitud_str;

        int offset_net = htonl(*valor);
        iov[nelem].iov_base=&offset_net;
        iov[nelem++].iov_len=sizeof(int);

        if(!conectado){
            if((s=init_socket_client(getenv("BROKER_HOST"),getenv("BROKER_PORT")))<0)
                return -1;
            conectado=true;
        }
        //Envío
        if (writev(s, iov, 4)<0){
            perror("Error en envío msg_length");
            conectado=false;
            close(s);
            return -1;
        }
        //Respuesta
        if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
            perror("Error en recibir tam_mensaje");
            close(s);
            return -1;
        }

        longitud = ntohl(res);
        printf("Longitud: %d\n",longitud);

        if(longitud>0){
            mensaje = malloc(longitud);
            if(recv(s, mensaje, longitud, MSG_WAITALL) != longitud) {
                perror("Error en recibir mensaje");
                close(s);
                return -1;
            }
            if(map_remove_entry(recorrido_poll,clave,NULL)==-1)
                printf("Error, tema no existe en mapa al eliminar\n");
            
            *valor = *valor + 1;
            
            if(map_put(recorrido_poll,strdup(clave),valor)==-1)
                printf("Error, tema no existe en mapa al insertar\n");
            encontrado = true;
        }
        map_iter_next(it);
    }

    pos = map_iter_exit(it);
    *topic = strdup(clave);
    *msg = mensaje; 
    return longitud;
}

// QUINTA FASE: COMMIT OFFSETS

// Cliente guarda el offset especificado para ese tema.
// Devuelve 0 si OK y un número negativo en caso de error.
int commit(char *client, char *topic, int offset) {
    int err;
    int *offset_client = malloc(sizeof(int));
    offset_client=map_get(suscripciones,topic,&err);
    offset = *offset_client;
    struct iovec iov[6];
    int nelem = 0;

    int entero_netc = htonl(6);
    iov[nelem].iov_base=&entero_netc;
    iov[nelem++].iov_len=sizeof(int);

    int longitud_strt = strlen(topic);
    int longitud_str_nett = htonl(longitud_strt);
    iov[nelem].iov_base=&longitud_str_nett;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic; 
    iov[nelem++].iov_len=longitud_strt;

    int longitud_strc = strlen(client);
    int longitud_str_netc = htonl(longitud_strc);
    iov[nelem].iov_base=&longitud_str_netc;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=client; 
    iov[nelem++].iov_len=longitud_strc;

    int entero_neto = htonl(offset);
    iov[nelem].iov_base=&entero_neto;
    iov[nelem++].iov_len=sizeof(int);

    if(!conectado){
        if((s=init_socket_client(getenv("BROKER_HOST"),getenv("BROKER_PORT")))<0)
            return -1;
        conectado=true;
    }
    //Envío
    if (writev(s, iov, 6)<0){
        perror("Error en envío commit");
        conectado=false;
        close(s);
        return -1;
    }
    //Respuesta
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        perror("Error en recivir commit");
        close(s);
        return -1;
    }
    return ntohl(res);
}


// Cliente obtiene el offset guardado para ese tema.
// Devuelve el offset y un número negativo en caso de error.
int commited(char *client, char *topic) {
    int err;
    int *offset_client = malloc(sizeof(int));
    offset_client=map_get(suscripciones,topic,&err);
    
    struct iovec iov[5];
    int nelem = 0;

    int entero_net = htonl(7);
    iov[nelem].iov_base=&entero_net;
    iov[nelem++].iov_len=sizeof(int);

    int longitud_strt = strlen(topic);
    int longitud_str_nett = htonl(longitud_strt);
    iov[nelem].iov_base=&longitud_str_nett;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=topic; 
    iov[nelem++].iov_len=longitud_strt;

    int longitud_strc = strlen(client);
    int longitud_str_netc = htonl(longitud_strc);
    iov[nelem].iov_base=&longitud_str_netc;
    iov[nelem++].iov_len=sizeof(int);
    iov[nelem].iov_base=client; 
    iov[nelem++].iov_len=longitud_strc;

    if(!conectado){
        if((s=init_socket_client(getenv("BROKER_HOST"),getenv("BROKER_PORT")))<0)
            return -1;
        conectado=true;
    }
    //Envío
    if (writev(s, iov, 5)<0){
        perror("Error en envío commited");
        conectado=false;
        close(s);
        return -1;
    }
    //Respuesta
    if (recv(s, &res, sizeof(int), MSG_WAITALL) != sizeof(int)) {
        perror("Error en recivir commited");
        close(s);
        return -1;
    }
    *offset_client = ntohl(res);
    map_put(suscripciones,strdup(topic),offset_client);
    return *offset_client;
}