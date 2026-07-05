package replicacion

import (
	"sync"
)

// Estructuras de mensajes RPC.
// Campos necesarios:
//   ArgsEscritura: Clave, Valor, Timestamp
//   RespEscritura: Exito, NodoID
//   ArgsLectura:   Clave
//   RespLectura:   Valor, Timestamp, NodoID

type ArgsEscritura struct {
	Clave     string
	Valor     string
	Timestamp int64
}
type RespEscritura struct {
	Exito  bool
	NodoID string
}
type ArgsLectura struct {
	Clave string
}
type RespLectura struct {
	Valor      string
	Timestamp  int64
	NodoID     string
	Encontrado bool
}

// TODO 1: Definir QuorumConfig con N, W, R.
// Agregar metodo Validar() bool que retorne W+R > N.
type QuorumConfig struct {
	N int
	W int
	R int
}

// Todo 2: Store es el almacenamiento local con timestamps.
type Store struct {
	// COMPLETAR
}

// TODO 3: Implementar NuevoStore.
func NuevoStore() *Store {
	// COMPLETAR
	return nil
}

// TODO 4: Implementar Escribir.
// Si el timestamp recibido es mayor o igual al almacenado, actualizar.
// Retornar true si se actualizo, false si se ignoro.
func (s *Store) Escribir(clave, valor string, timestamp int64) bool {
	// COMPLETAR
	return false
}

// TODO 5: Implementar Leer.
// Retornar valor, timestamp y un bool indicando si la clave existe.
func (s *Store) Leer(clave string) (string, int64, bool) {
	// STUB
	return "", 0, false
}

// TODO 6: Implementar Sincronizar.
// Misma lógica que Escribir (es idempotente).
// Se usa para read-repair.
func (s *Store) Sincronizar(clave, valor string, timestamp int64) bool {
	// COMPLETAR
	return false
}

// ServicioQuorum expone métodos RPC para lecturas y escrituras con quorum.
type ServicioQuorum struct {
	NodoID string
	Store  *Store
	Pares  []string
	Config QuorumConfig
}

// TODO 7: Implementar Escribir (RPC).
// Recibe ArgsEscritura, delega en Store.Escribir, responde con Exito=true y NodoID.
func (s *ServicioQuorum) Escribir(args ArgsEscritura, resp *RespEscritura) error {
	// COMPLETAR
	return nil
}

// TODO 8: Implementar Leer (RPC).
// Recibe ArgsLectura, delega en Store.Leer, responde con valor, timestamp y NodoID.
func (s *ServicioQuorum) Leer(args ArgsLectura, resp *RespLectura) error {
	// COMPLETAR
	return nil
}

// TODO 9: Implementar Sincronizar (RPC).
// Recibe ArgsEscritura, delega en Store.Sincronizar para read-repair.
func (s *ServicioQuorum) Sincronizar(args ArgsEscritura, resp *RespEscritura) error {
	// COMPLETAR
	return nil
}

// CoordinarEscritura es la funcion cliente que coordina el quorum de escritura.
// Conecta RPC a cada par, invoca Escribir, y retorna true si W o mas confirmaron.
// TODO 10: Implementar CoordinarEscritura.
func CoordinarEscritura(clave, valor string, timestamp int64, pares []string, w int) bool {
	// COMPLETAR
	return false
}

// CoordinarLectura es la funcion cliente que coordina el quorum de lectura.
// Conecta RPC a cada par, invoca Leer, y retorna el valor con el timestamp mas grande.
// Retorna true si obtuvo al menos R respuestas.
// TODO 11: Implementar CoordinarLectura.
func CoordinarLectura(clave string, pares []string, r int) (string, int64, bool) {
	// COMPLETAR
	return "", 0, false
}

