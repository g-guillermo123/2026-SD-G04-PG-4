// Reutilización de pkg/gossip/nodo.go proveniente del PG3
// Se eliminó el método "Identificarse" y los tipos "ArgsVacio" y "RespIdentificacion"
package gossip

import (
	"math/rand"
	"sync"
)

// TODO 1: Definir el struct NodoGossip.
// Debe mantener una lista de miembros (map[string]bool) protegida por sync.RWMutex.
// Campos sugeridos:
//   - ID int
//   - Dirección string (ej. "localhost:5000")
//   - Miembros map[string]bool (todos los nodos conocidos, incluido si mismo)
//   - mu sync.RWMutex

type NodoGossip struct {
	ID        int
	Direccion string
	Miembros  map[string]bool
	mu        sync.RWMutex
}

// TODO 2: Implementar NuevoNodo.
// Crear un nodo con ID y Direccion dados. Inicializar Miembros con solo la Direccion propia.
func NuevoNodo(id int, direccion string) *NodoGossip {
	return &NodoGossip{
		ID:        id,
		Direccion: direccion,
		Miembros: map[string]bool{
			direccion: true,
		},
	}
}

// TODO 3: Implementar Unirse.
// Agrega una Direccion a la lista de miembros (protegido con mutex).
func (n *NodoGossip) Unirse(direccion string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Miembros[direccion] = true
}

// TODO 4: Implementar AntiEntropia.
// Devuelve la Direccion de un par aleatorio de Miembros (distinto de si mismo).
// Si no hay otros miembros, retorna "".
// Esta función sera llamada periódicamente por el main para realizar el RPC.
// Pasos:
//  1. Adquirir RLock sobre n.mu.
//  2. Iterar n.Miembros buscando una Direccion distinta de n.Direccion.
//  3. Si se encuentra, retornarla. Si no, retornar "".
func (n *NodoGossip) AntiEntropia() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var candidatos []string
	for dir, vivo := range n.Miembros {
		if vivo && dir != n.Direccion {
			candidatos = append(candidatos, dir)
		}
	}

	if len(candidatos) == 0 {
		return ""
	}

	// Selección aleatoria epidémica clásica
	return candidatos[rand.Intn(len(candidatos))]
}

// TODO 5: Implementar ObtenerMiembros.
// Devuelve una copia de la lista de miembros (protegida con RLock).
func (n *NodoGossip) ObtenerMiembros() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var lista []string
	for dir, vivo := range n.Miembros {
		if vivo {
			lista = append(lista, dir)
		}
	}
	return lista
}

// TODO 6: Implementar FusionarMiembros.
// Recibe un slice de direcciones y las agrega a Miembros.
func (n *NodoGossip) FusionarMiembros(nuevos []string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, dir := range nuevos {
		if dir != "" {
			n.Miembros[dir] = true
		}
	}
}

// Intercambio es la estructura usada en RPC para intercambiar miembros.
type Intercambio struct {
	Remitente string
	Miembros  []string
}

// ServicioGossip es el servicio RPC para Gossip.
type ServicioGossip struct {
	Nodo *NodoGossip
}

// Intercambiar recibe los miembros de otro nodo y devuelve los propios.
// TODO 7: implementar anti-entropía (push-pull).
func (s *ServicioGossip) Intercambiar(req Intercambio, resp *Intercambio) error {
	// 1. PUSH: Fusionar en nuestra memoria los miembros que nos envía el emisor
	s.Nodo.FusionarMiembros(req.Miembros)

	// 2. PULL: Cargar nuestro estado de miembros actualizados para devolvérselo
	resp.Remitente = s.Nodo.Direccion
	resp.Miembros = s.Nodo.ObtenerMiembros()

	return nil
}
