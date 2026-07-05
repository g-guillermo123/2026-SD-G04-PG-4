package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"

	"sd-datastore/pkg/gossip"
	"sd-datastore/pkg/replicacion"
)

var (
	idNodo     string
	puertoHTTP string
	puertoRPC  string
	pares      []string
	gossipNodo *gossip.NodoGossip
	servicioQ  *replicacion.ServicioQuorum
	configQ    replicacion.QuorumConfig
)

func main() {
	idNodo = os.Getenv("NODO_ID")
	if idNodo == "" {
		idNodo = "1"
	}
	puertoHTTP = os.Getenv("HTTP_PORT")
	if puertoHTTP == "" {
		puertoHTTP = "8080"
	}
	puertoRPC = os.Getenv("RPC_PORT")
	if puertoRPC == "" {
		puertoRPC = "5000"
	}

	pares = parsearPares(os.Getenv("PEERS"))

	// TODO 12: Parsear QUORUM_N, QUORUM_W, QUORUM_R de las variables de entorno.
	// Valores por defecto: N=3, W=2, R=2.

	// Parsear las variables de entorno y asignar valores por defecto
	n, errN := strconv.Atoi(os.Getenv("QUORUM_N"))
	if errN != nil {
		n = 3 // Valores por defecto para el quorum (N, W, R)
	}
	w, errW := strconv.Atoi(os.Getenv("QUORUM_W"))
	if errW != nil {
		w = 2
	}
	r, errR := strconv.Atoi(os.Getenv("QUORUM_R"))
	if errR != nil {
		r = 2
	}

	configQ = replicacion.QuorumConfig{N: n, W: w, R: r}
	if !configQ.Validar() {
		log.Fatalf("Configuración de Quorum inválida (W+R debe ser > N)")
	}

	idNum, _ := strconv.Atoi(idNodo)
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}
	miDireccionRPC := fmt.Sprintf("%s:%s", hostname, puertoRPC)

	// Inicializar gossip
	gossipNodo = gossip.NuevoNodo(idNum, miDireccionRPC)

	seed := os.Getenv("SEED")
	if seed != "" {
		gossipNodo.Unirse(seed)
	}

	// TODO 13: Inicializar Store, ServicioQuorum y QuorumConfig.
	storeLocal := replicacion.NuevoStore()
	servicioQ = &replicacion.ServicioQuorum{
		NodoID: idNodo,
		Store:  storeLocal,
		Pares:  pares,
		Config: configQ,
	}

	// Endpoints HTTP
	http.HandleFunc("/estado", manejadorEstado)
	http.HandleFunc("/datos/", manejadorDatos)

	// Servicio RPC
	go iniciarRPC()

	// Loop anti-entropia
	go bucleAntiEntropia()

	addr := ":" + puertoHTTP
	fmt.Printf("[NODO %s] Escuchando HTTP en %s, RPC en %s\n", idNodo, addr, puertoRPC)
	log.Fatal(http.ListenAndServe(addr, nil))
}

// TODO 12b: Implementar parsearPares (usen de PG3).
// Convierte "1=host:port,2=host:port,..." en []string con direcciones RPC.
func parsearPares(peersEnv string) []string {
	if peersEnv == "" {
		return []string{}
	}
	var listaPares []string
	// Separamos por coma cada bloque de nodo
	partes := strings.Split(peersEnv, ",")
	for _, parte := range partes {
		// Separamos el ID de la dirección (ej: "1=localhost:5001")
		subPartes := strings.Split(parte, "=")
		if len(subPartes) == 2 {
			listaPares = append(listaPares, subPartes[1])
		} else {
			// Por si viene directo sin "ID="
			listaPares = append(listaPares, parte)
		}
	}
	return listaPares
}

// TODO 14: Implementar iniciarRPC.
// Crear listener TCP, registrar ServicioGossip y ServicioQuorum, atender conexiones.
func iniciarRPC() {
	// 1. Registrar los servicios en el servidor RPC por defecto de Go
	err := rpc.Register(servicioQ)
	if err != nil {
		log.Fatalf("Error registrando ServicioQuorum en RPC: %v", err)
	}
	err = rpc.Register(&gossip.ServicioGossip{Nodo: gossipNodo}) // Registrar ServicioGossip para RPC
	if err != nil {
		log.Fatalf("Error registrando ServicioGossip en RPC: %v", err)
	}

	// 2. Abrir el socket TCP de escucha
	listener, err := net.Listen("tcp", ":"+puertoRPC)
	if err != nil {
		log.Fatalf("Error levantando puerto TCP para RPC: %v", err)
	}
	defer listener.Close()

	// 3. Loop infinito atendiendo clientes RPC de forma nativa
	rpc.Accept(listener)
}

// TODO 15: Implementar bucleAntiEntropia.
// Cada 5 segundos obtener un par con gossipNodo.AntiEntropia(),
// conectarse via RPC, intercambiar miembros y fusionar.
func bucleAntiEntropia() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Loop infinito para anti-entropía
	for {
		select {
		case <-ticker.C:
			// Obtener un par aleatorio usando AntiEntropia() (importado de gossip)
			vecinoDireccion := gossipNodo.AntiEntropia()
			if vecinoDireccion == "" {
				continue // No hay vecinos conocidos aún
			}

			// Conectarse vía RPC al vecino elegido
			cliente, err := rpc.Dial("tcp", vecinoDireccion)
			if err != nil {
				continue
			}

			// Intercambiar listas de miembros (push-pull) usando el servicio RPC
			req := gossip.Intercambio{Remitente: gossipNodo.Direccion, Miembros: gossipNodo.ObtenerMiembros()}
			var resp gossip.Intercambio

			err = cliente.Call("ServicioGossip.Intercambiar", req, &resp)
			cliente.Close()

			if err == nil {
				// Fusionar la información recibida con nuestro estado local
				gossipNodo.FusionarMiembros(resp.Miembros)
			}
		}
	}
}

// manejadorEstado responde GET /estado con informacion del nodo.
func manejadorEstado(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]interface{}{
		"node_id":  idNodo,
		"miembros": gossipNodo.ObtenerMiembros(),
		"quorum": map[string]int{
			"N": configQ.N,
			"W": configQ.W,
			"R": configQ.R,
		},
		"pares": pares,
	})
}

// manejadorDatos maneja PUT /datos/{clave} y GET /datos/{clave}.
// actualizar de acuerdo a lo implementado
func manejadorDatos(w http.ResponseWriter, r *http.Request) {
	// (actualizado)
	partes := strings.Split(strings.TrimPrefix(r.URL.Path, "/datos/"), "/")
	if len(partes) == 0 || partes[0] == "" {
		http.Error(w, "falta clave", http.StatusBadRequest)
		return
	}
	clave := partes[0]

	// Manejar los métodos HTTP PUT y GET para la clave especificada
	switch r.Method {
	case http.MethodPut:
		var body struct {
			Valor string `json:"valor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Timestampt único basado en Unix Nano (Garantiza orden temporal estricto)
		timestampActual := time.Now().UnixNano()

		// Coordina la escritura distribuida exigiendo la cuota W
		exitoQuorum := replicacion.CoordinarEscritura(clave, body.Valor, timestampActual, pares, configQ.W)

		if !exitoQuorum {
			http.Error(w, "Error: No se alcanzó el quorum de escritura (W)", http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "escrito exitosamente por quorum"})

	case http.MethodGet:
		// Coordina la lectura distribuida exigiendo la cuota R (Trae el dato más nuevo)
		valor, ts, encontrado := replicacion.CoordinarLectura(clave, pares, configQ.R)

		if !encontrado {
			http.Error(w, "Clave no encontrada en el sistema por Quorum", http.StatusNotFound)
			return
		}

		// Responder con éxito devolviendo los datos y metadatos de replicación
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"clave":     clave,
			"valor":     valor,
			"timestamp": ts,
		})

	default:
		http.Error(w, "metodo no soportado", http.StatusMethodNotAllowed)
	}
}
