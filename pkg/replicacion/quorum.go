package replicacion

import (
	"net/rpc"
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
	NodoAddr   string
	Encontrado bool
}

// TODO 1: Definir QuorumConfig con N, W, R.
// Agregar metodo Validar() bool que retorne W+R > N.
type QuorumConfig struct {
	N int
	W int
	R int
}

// Implementación del método Validar() para QuorumConfig.
func (q QuorumConfig) Validar() bool {
	return (q.W + q.R) > q.N
}

// Todo 2: Store es el almacenamiento local con timestamps.
type Registro struct {
	Valor     string
	Timestamp int64
}

// Implementación de Store con un mapa protegido por un mutex para acceso concurrente.
type Store struct {
	datos map[string]Registro
	mu    sync.RWMutex // Protege el acceso concurrente al mapa
}

// TODO 3: Implementar NuevoStore.
// Implementación de función NuevoStore que inicializa un Store con un mapa vacío.
func NuevoStore() *Store {
	return &Store{
		datos: make(map[string]Registro),
	}
}

// TODO 4: Implementar Escribir.
// Si el timestamp recibido es mayor o igual al almacenado, actualizar.
// Retornar true si se actualizo, false si se ignoro.
func (s *Store) Escribir(clave, valor string, timestamp int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Verificar si la clave ya existe y comparar timestamps
	regExistente, existe := s.datos[clave]
	// Si ya existe y el timestamp entrante es MENOR, se ignora (Last-Write-Wins)
	if existe && timestamp < regExistente.Timestamp {
		return false
	}

	// Si no existe o el timestamp es mayor/igual, se actualiza
	s.datos[clave] = Registro{
		Valor:     valor,
		Timestamp: timestamp,
	}
	return true
}

// TODO 5: Implementar Leer.
// Retornar valor, timestamp y un bool indicando si la clave existe.
func (s *Store) Leer(clave string) (string, int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Verificar si la clave existe en el mapa
	reg, existe := s.datos[clave]
	if !existe {
		return "", 0, false
	}
	return reg.Valor, reg.Timestamp, true
}

// TODO 6: Implementar Sincronizar.
// Misma lógica que Escribir (es idempotente).
// Se usa para read-repair.
func (s *Store) Sincronizar(clave, valor string, timestamp int64) bool {
	// Al ser idempotente, simplemente llamo a Escribir
	return s.Escribir(clave, valor, timestamp)
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
	// Delegar en Store.Escribir
	s.Store.Escribir(args.Clave, args.Valor, args.Timestamp)
	// Responder con Exito=true y NodoID
	resp.Exito = true
	resp.NodoID = s.NodoID
	return nil
}

// TODO 8: Implementar Leer (RPC).
// Recibe ArgsLectura, delega en Store.Leer, responde con valor, timestamp y NodoID.
func (s *ServicioQuorum) Leer(args ArgsLectura, resp *RespLectura) error {
	// Llamada a Store.Leer para obtener el valor, timestamp y existencia de la clave
	valor, ts, existe := s.Store.Leer(args.Clave)
	// Rellenar la respuesta con los datos obtenidos
	resp.Valor = valor
	resp.Timestamp = ts
	resp.NodoID = s.NodoID
	resp.Encontrado = existe
	return nil
}

// TODO 9: Implementar Sincronizar (RPC).
// Recibe ArgsEscritura, delega en Store.Sincronizar para read-repair.
func (s *ServicioQuorum) Sincronizar(args ArgsEscritura, resp *RespEscritura) error {
	// Llamada a Store.Sincronizar para actualizar el valor y timestamp de la clave
	s.Store.Sincronizar(args.Clave, args.Valor, args.Timestamp)
	// Respondo con Exito (true) y NodoID
	resp.Exito = true
	resp.NodoID = s.NodoID
	return nil
}

// CoordinarEscritura es la funcion cliente que coordina el quorum de escritura.
// Conecta RPC a cada par, invoca Escribir, y retorna true si W o mas confirmaron.
// TODO 10: Implementar CoordinarEscritura.
func CoordinarEscritura(clave, valor string, timestamp int64, pares []string, w int) bool {
	// Canal para recibir confirmaciones de éxito en segundo plano
	canalConfirmaciones := make(chan bool, len(pares))
	args := ArgsEscritura{Clave: clave, Valor: valor, Timestamp: timestamp}

	// Lanzar llamadas RPC concurrentes a cada par
	for _, par := range pares {
		go func(direccion string) {
			cliente, err := rpc.Dial("tcp", direccion)
			if err != nil {
				canalConfirmaciones <- false
				return
			}
			defer cliente.Close()

			// Llamada RPC a Escribir en el par
			var resp RespEscritura
			err = cliente.Call("ServicioQuorum.Escribir", args, &resp)
			if err != nil {
				canalConfirmaciones <- false
			} else {
				canalConfirmaciones <- resp.Exito
			}
		}(par)
	}

	// Contador de votos positivos recibidos
	votosPositivos := 1
	// Contamos el voto positivo del nodo local (asumimos que la escritura local siempre es exitosa)
	// Procesamos el canal
	for i := 0; i < len(pares); i++ {
		if <-canalConfirmaciones {
			votosPositivos++
		}
		// Si se alcanza el quorum de escritura, retornamos true (éxito)
		if votosPositivos >= w {
			return true
		}
	}

	return votosPositivos >= w
}

// CoordinarLectura es la funcion cliente que coordina el quorum de lectura.
// Conecta RPC a cada par, invoca Leer, y retorna el valor con el timestamp mas grande.
// Retorna true si obtuvo al menos R respuestas.
// TODO 11: Implementar CoordinarLectura.
func CoordinarLectura(clave string, pares []string, r int, local *Store) (string, int64, bool) {
	canalLecturas := make(chan RespLectura, len(pares))
	args := ArgsLectura{Clave: clave}

	// Variables para recolectar respuestas y determinar el mejor valor
	var respuestasRecibidas []RespLectura
	respuestasValidas := 0

	var mejorValor string
	var maxTimestamp int64
	encontradoGlobal := false

	// Si tenemos una copia local, consultarla primero y agregarla a las respuestas
	if local != nil {
		if v, ts, ok := local.Leer(clave); ok {
			respuestasRecibidas = append(respuestasRecibidas, RespLectura{Valor: v, Timestamp: ts, NodoAddr: "local", Encontrado: true})
			respuestasValidas++
			encontradoGlobal = true
			maxTimestamp = ts
			mejorValor = v
		}
	}

	// Lanzar llamadas RPC concurrentes a pares remotos
	for _, par := range pares {
		go func(direccion string) {
			cliente, err := rpc.Dial("tcp", direccion)
			if err != nil {
				canalLecturas <- RespLectura{Encontrado: false, NodoAddr: direccion}
				return
			}
			defer cliente.Close()

			var resp RespLectura
			err = cliente.Call("ServicioQuorum.Leer", args, &resp)
			if err != nil {
				canalLecturas <- RespLectura{Encontrado: false, NodoAddr: direccion}
			} else {
				// Guardamos la dirección remota para poder realizar read-repair más tarde
				resp.NodoAddr = direccion
				canalLecturas <- resp
			}
		}(par)
	}

	// Recolectar resultados de la red
	for i := 0; i < len(pares); i++ {
		resp := <-canalLecturas
		respuestasRecibidas = append(respuestasRecibidas, resp)

		if resp.Encontrado {
			respuestasValidas++
			encontradoGlobal = true
			if resp.Timestamp > maxTimestamp {
				maxTimestamp = resp.Timestamp
				mejorValor = resp.Valor
			}
		}
	}

	// Si no llegamos al quorum de lectura (R), la operación falla
	if respuestasValidas < r {
		return "", 0, false
	}

	// Read-Repair (sincronizar desactualizados)
	for _, resp := range respuestasRecibidas {
		if resp.Encontrado && resp.Timestamp < maxTimestamp {
			if resp.NodoAddr == "local" {
				continue
			}
			go func(nodoAddr string) {
				cliente, err := rpc.Dial("tcp", nodoAddr)
				if err != nil {
					return
				}
				defer cliente.Close()
				argsSinc := ArgsEscritura{Clave: clave, Valor: mejorValor, Timestamp: maxTimestamp}
				var respSinc RespEscritura
				_ = cliente.Call("ServicioQuorum.Sincronizar", argsSinc, &respSinc)
			}(resp.NodoAddr)
		}
	}

	return mejorValor, maxTimestamp, encontradoGlobal
}
