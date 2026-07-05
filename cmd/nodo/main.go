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
	idNodo      string
	puertoHTTP  string
	puertoRPC   string
	pares       []string
	gossipNodo  *gossip.NodoGossip
	servicioQ   *replicacion.ServicioQuorum
	configQ     replicacion.QuorumConfig
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
	// COMPLETAR
	return nil
}

// TODO 14: Implementar iniciarRPC.
// Crear listener TCP, registrar ServicioGossip y ServicioQuorum, atender conexiones.
func iniciarRPC() {
	// COMPLETAR
}

// TODO 15: Implementar bucleAntiEntropia.
// Cada 5 segundos obtener un par con gossipNodo.AntiEntropia(),
// conectarse via RPC, intercambiar miembros y fusionar.
func bucleAntiEntropia() {
	// COMPLETAR
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
	partes := strings.Split(strings.TrimPrefix(r.URL.Path, "/datos/"), "/")
	if len(partes) == 0 || partes[0] == "" {
		http.Error(w, "falta clave", http.StatusBadRequest)
		return
	}
	clave := partes[0]

	switch r.Method {
	case http.MethodPut:
		var body struct {
			Valor string `json:"valor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = clave
		_ = body
		http.Error(w, "no implementado", http.StatusNotImplemented)

	case http.MethodGet:
		_ = clave
		http.Error(w, "no implementado", http.StatusNotImplemented)

	default:
		http.Error(w, "metodo no soportado", http.StatusMethodNotAllowed)
	}
}
