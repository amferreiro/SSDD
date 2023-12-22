#include <stdio.h>

#include "comun.h"
#include "map.h"
#include "queue.h"

#include <stdbool.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>


// información que se la pasa el thread creado
typedef struct thread_info {
    int socket; // añadir los campos necesarios
} thread_info;


typedef struct tema {
    char* nombretema;
    queue *mensajes;
}tema;


typedef struct mensaje {
    int tam;
    unsigned char* mensaje;
}mensaje;


map *mapatemas;
tema *t;
mensaje *m;
char* path;


int recibir_entero(void *arg){
    int entero;
    thread_info *thinf = arg;
    // cada "petición" comienza con un entero
    if (recv(thinf->socket, &entero, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return -1;
    entero = ntohl(entero);
    printf("Recibido entero: %d\n", entero);
    return entero;
}

//Funcion que recibe un string del cliente
char* recibir_string(void *arg){
    int longitud;
    char* string;
    thread_info *thinf = arg;
    if(recv(thinf->socket, &longitud, sizeof(int), MSG_WAITALL)!=sizeof(int)){
        printf("Error al recibir longitud: %d\n", longitud);
        return NULL;
    }
    longitud = ntohl(longitud);
    string = malloc(longitud+1); // +1 para el carácter nulo
    // ahora sí llega el string
    if (recv(thinf->socket, string, longitud, MSG_WAITALL)!=longitud){
        printf("Error al recibir string\n");
        return NULL;
    }
    string[longitud]='\0';       // añadimos el carácter nulo
    printf("Recibido string: %s\n", string);
    return string;
}

//Funcion que reibe un array del cliente
mensaje* recibir_array(void *arg){
    int longitud;
    unsigned char *array_binario;
    thread_info *thinf = arg;
    if (recv(thinf->socket, &longitud, sizeof(int), MSG_WAITALL)!=sizeof(int))
        return NULL;
    longitud = ntohl(longitud);
    array_binario = malloc(longitud); // no usa un terminador
    // llega el array
    if (recv(thinf->socket, array_binario, longitud, MSG_WAITALL)!=longitud)
        return NULL;
    printf("Recibido msg: ");
    for (int i=0; i<longitud; i++) printf("%02x", array_binario[i]);
    printf("\n");
    mensaje *m=malloc(sizeof(mensaje));
    m->tam=longitud;
    m->mensaje=array_binario;
    return m;
}



int do_create_topic(void *arg) {
    char *topic=NULL;
    topic=recibir_string(arg);

    //Rellena el struct de tema 
    t = malloc(sizeof(tema));
    t->nombretema=topic;
    t->mensajes=queue_create(0);

    printf("Nombre del tema: %s\n",t->nombretema);

    //Si da error el put, tema ya existe.
    if(map_put(mapatemas,t->nombretema,t)<0)
        return -1;
    else
        printf("Tema: %s metido en el mapa\n", t->nombretema);
    return 0;
}

int do_n_topic(){
    int n;
    return n=map_size(mapatemas);
    printf("Numero de temas: %d\n",n);
}

//revisar mensajes con nulo
int do_send_message(void *arg) {
    char *topic;
    int err;
    int offset;
    t=malloc(sizeof(tema));
    m=malloc(sizeof(mensaje));

    topic=recibir_string(arg);
    m = recibir_array(arg);
    
    t=map_get(mapatemas,topic,&err);
    if(err<0)
        return -1;

    queue_append(t->mensajes,m);
    offset=queue_size(t->mensajes);
    if(offset<0)
        return -1;
    return offset;
}


int do_msg_length(void *arg) {
    int offset;
    int err;
    char *topic=NULL;

    topic=recibir_string(arg);
    offset=recibir_entero(arg);

    t=malloc(sizeof(tema));
    m=malloc(sizeof(mensaje));

    t = map_get(mapatemas,topic,&err);
    if (err<0)
        return -1;
    m = queue_get(t->mensajes,offset,&err);
    if(err<0)
        return 0;
    return m->tam;
}



int do_end_offset (void *arg) {
    int tam_cola;
    int err;
    char *topic=NULL;
    t = malloc(sizeof(tema));
    topic=recibir_string(arg);
    t=map_get(mapatemas,topic,&err);
    if(err<0)
        return -1;
    tam_cola=queue_size(t->mensajes);
    return tam_cola;
}

mensaje* do_poll (void *arg) {
    int offset;
    int err;
    char *topic=NULL;
    mensaje *mensaje_poll=malloc(sizeof(mensaje));
    
    mensaje *mensaje_nulo=malloc(sizeof(mensaje));
    mensaje_nulo->tam=0;
    mensaje_nulo->mensaje=NULL;

    t = malloc(sizeof(tema));
    topic=recibir_string(arg);
    t = map_get(mapatemas,topic,&err);
    if(err<0){
        printf("Tema no encontrado\n");
        mensaje_poll=mensaje_nulo;
    }
    offset=recibir_entero(arg);
    mensaje_poll = queue_get(t->mensajes,offset,&err);
    if(err<0){
        printf("No hay mensaje: Error: %d\n",err);
        mensaje_poll=mensaje_nulo;
    }
    return mensaje_poll;
}

int do_commit(void *arg,char* path){
    printf("Committing\n");
    char *topic=NULL;
    char *cliente=NULL;
    int offset;
    char *path_completo;
    int err;
    topic=recibir_string(arg);
    cliente=recibir_string(arg);
    offset=recibir_entero(arg);
    printf("Recibidos los datos del cliente\n");
    t=malloc(sizeof(tema));
    t=map_get(mapatemas,topic,&err);
    if(err<0)
        return -1;

    int longitud = strlen(path)+strlen(cliente)+2;
    path_completo = malloc(longitud);
    strcpy(path_completo,path);
    strcat(path_completo,"/");
    strcat(path_completo,cliente);
    DIR* dir = opendir(path_completo);
    printf("Empieza a crear el directorio: %s\n",path_completo);

    if (!dir){
        printf("No existe el directorio: %s\n",path_completo);
        mkdir(path_completo, 0700);
    }
    longitud = strlen(path)+strlen(cliente)+strlen(topic)+2;
    path_completo = malloc(longitud);
    strcpy(path_completo,path);
    strcat(path_completo,"/");
    strcat(path_completo,cliente);
    strcat(path_completo,"/");
    strcat(path_completo,topic);
    printf("Path completo: %s\n",path_completo);
    FILE* archivo = fopen(path_completo, "w");
    if (archivo == NULL) {
        printf("No se pudo crear el archivo\n");
        return -1;
    }
    fprintf(archivo, "%d", offset);
    fclose(archivo);
    free(path_completo);
    printf("Archivo creado y escrito correctamente.\n");
    return offset;
}

int do_commited(void *arg,char* path){
    char *topic=NULL;
    char *cliente=NULL;
    int offset;
    int err;
    char *path_completo;
    topic=recibir_string(arg);
    cliente=recibir_string(arg);
    path_completo=malloc(strlen(path)+strlen(cliente)+strlen(topic)+2);
    t=malloc(sizeof(tema));
    t=map_get(mapatemas,topic,&err);
    if(err<0)
        return -1;
    strcpy(path_completo,path);
    strcat(path_completo,"/");
    strcat(path_completo,cliente);
    strcat(path_completo,"/");
    strcat(path_completo,topic);
    FILE* archivo = fopen(path_completo, "r");
    if (archivo == NULL) {
        printf("No se pudo leer el archivo\n");
        return 1;
    }
    if (fscanf(archivo, "%d", &offset) == 1) 
        printf("Offset obtenido correctament del fichero%d\n",offset);
    fclose(archivo);
    return offset;
}


void *servicio(void *arg){
    int entero=0;
    bool send_msg=false;
    mensaje* str_msg;
    thread_info *thinf = arg;
    while (1) {
        int res;
        entero = recibir_entero(arg);
        switch (entero){
            case 0: //create_topic
                res = do_create_topic(arg);
                printf("Res = %d\n",res);
                printf("create_topic done\n");
                send_msg=false;
                break;
            case 1://n_topic
                res = do_n_topic();
                printf("Res = %d\n",res);
                printf("n_topic done\n");
                send_msg=false;
                break;
            case 2://send_message
                res = do_send_message(arg);
                printf("Res = %d\n",res);
                printf("send_message done\n");
                send_msg=false;
                break;
            case 3://msg_legth
                res = do_msg_length(arg);
                printf("Res = %d\n",res);
                printf("msg_length done\n");
                send_msg=false;
                break;
            case 4:
                res = do_end_offset(arg);
                printf("Res = %d\n",res);
                printf("end_offset done\n");
                send_msg=false;
                break;
            case 5:
                str_msg=do_poll(arg);
                printf("Tam_poll %d\n",str_msg->tam);
                if(str_msg->tam==0){
                    printf("No hay mensaje\n");
                    write(thinf->socket, &str_msg->tam, sizeof(int));
                    send_msg = true;
                    break;
                }

                struct iovec iov[2];
                int nelem = 0;
                int longitud_arr_net = htonl(str_msg->tam);
                iov[nelem].iov_base=&longitud_arr_net;
                iov[nelem++].iov_len=sizeof(int);
                iov[nelem].iov_base=str_msg->mensaje;
                iov[nelem++].iov_len=str_msg->tam;
                for (int i=0; i<str_msg->tam; i++) printf("%02x", str_msg->mensaje[i]);
                printf("\n");    
                writev(thinf->socket, iov, 2);
                printf("do_poll done\n");
                send_msg=true;
                break;
            case 6:
                res = do_commit(arg,path);
                printf("Res = %d\n",res);
                printf("do_commit done\n");
                send_msg=false;
                break;
            case 7:
                res = do_commited(arg,path);
                printf("Res = %d\n",res);
                printf("do_commited done\n");
                send_msg=false;
                break;
            default:
                send_msg=true;
                break;
        }
        if(!send_msg){
            int respuesta = htonl(res);
            write(thinf->socket, &respuesta, sizeof(int));
            printf("Sale de switch y envia respuesta\n");
        }
    }
    close(thinf->socket);
    return NULL;
}


// inicializa el socket y lo prepara para aceptar conexiones
static int init_socket_server(const char * port) {
    int s;
    struct sockaddr_in dir;
    int opcion=1;
    // socket stream para Internet: TCP
    if ((s=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        perror("error creando socket");
        return -1;
    }
    // Para reutilizar puerto inmediatamente si se rearranca el servidor
    if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opcion, sizeof(opcion))<0){
        perror("error en setsockopt");
        return -1;
    }
    // asocia el socket al puerto especificado
    dir.sin_addr.s_addr=INADDR_ANY;
    dir.sin_port=htons(atoi(port));
    dir.sin_family=PF_INET;
    if (bind(s, (struct sockaddr *)&dir, sizeof(dir)) < 0) {
        perror("error en bind");
        close(s);
        return -1;
    }
    // establece el nº máx. de conexiones pendientes de aceptar
    if (listen(s, 5) < 0) {
        perror("error en listen");
        close(s);
        return -1;
    }
    return s;
}

int main(int argc, char *argv[]) {
    int s, s_conec;
    unsigned int tam_dir;
    struct sockaddr_in dir_cliente;
    mapatemas = map_create(key_string,1);

    if (argc!=2 && argc!=3) {
        fprintf(stderr, "Uso: %s puerto [dir_commited]\n", argv[0]);
        return 1;
    }
    // inicializa el socket y lo prepara para aceptar conexiones
    if ((s=init_socket_server(argv[1])) < 0) return -1;
    path=malloc(strlen(argv[2])+1);
    strcpy(path, argv[2]);
    // prepara atributos adecuados para crear thread "detached"
    pthread_t thid;
    pthread_attr_t atrib_th;
    pthread_attr_init(&atrib_th); // evita pthread_join
    pthread_attr_setdetachstate(&atrib_th, PTHREAD_CREATE_DETACHED);

    while(1) {
        tam_dir=sizeof(dir_cliente);
        // acepta la conexión
        if ((s_conec=accept(s, (struct sockaddr *)&dir_cliente, &tam_dir))<0){
            perror("error en accept");
            close(s);
            return -1;
        }
        // crea el thread de servicio
        thread_info *thinf = malloc(sizeof(thread_info));
        thinf->socket=s_conec;
        pthread_create(&thid, &atrib_th, servicio, thinf);
    }
    free(t);
    free(m);
    close(s); 
    return 0;
}