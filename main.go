package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// BatchRequest define la estructura de la peticion de entrada con un lote de tareas
type BatchRequest struct {
	Tasks []string `json:"tasks" binding:"required"`
}

// TaskResult define la estructura del resultado de cada tarea individual procesada
type TaskResult struct {
	Task   string `json:"task"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
	Time   string `json:"time_taken"`
}

// processTask simula una tarea pesada de procesamiento de forma asincrona con manejo de errores
func processTask(taskName string, ch chan<- TaskResult, wg *sync.WaitGroup) {
	defer wg.Done()

	startTime := time.Now()

	// Simulamos una validacion: si la tarea incluye la palabra "error", simulamos un fallo
	if strings.Contains(strings.ToLower(taskName), "error") {
		time.Sleep(200 * time.Millisecond) // Tiempo antes de fallar
		duration := time.Since(startTime)

		ch <- TaskResult{
			Task:   taskName,
			Status: "Failed",
			Error:  "Error crítico: No se pudo procesar la tarea solicitada",
			Time:   fmt.Sprintf("%v", duration),
		}
		return
	}

	// Simula una operacion que consume tiempo de 500 milisegundos
	time.Sleep(500 * time.Millisecond)
	duration := time.Since(startTime)

	// Enviamos el resultado exitoso al canal
	ch <- TaskResult{
		Task:   taskName,
		Status: "Completed",
		Time:   fmt.Sprintf("%v", duration),
	}
}

func main() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	// Endpoint para procesar el lote de tareas concurrentemente
	r.POST("/api/v1/process-batch", func(c *gin.Context) {
		var req BatchRequest

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "El formato de tareas enviado no es valido"})
			return
		}

		numTasks := len(req.Tasks)
		resultChan := make(chan TaskResult, numTasks)
		var wg sync.WaitGroup

		totalStart := time.Now()

		// Disparamos una Goroutine por cada tarea en paralelo
		for _, task := range req.Tasks {
			wg.Add(1)
			go processTask(task, resultChan, &wg)
		}

		// Esperamos a que todas las Goroutines terminen para cerrar el canal
		go func() {
			wg.Wait()
			close(resultChan)
		}()

		// Recolectamos los resultados finales procesados del canal
		var results []TaskResult
		failedCount := 0
		successCount := 0

		for result := range resultChan {
			if result.Status == "Failed" {
				failedCount++
			} else {
				successCount++
			}
			results = append(results, result)
		}

		totalDuration := time.Since(totalStart)

		// Retornamos las respuestas con las estadisticas detalladas
		c.JSON(http.StatusOK, gin.H{
			"total_tasks": numTasks,
			"successful":  successCount,
			"failed":      failedCount,
			"total_time":  fmt.Sprintf("%v", totalDuration),
			"results":     results,
		})
	})

	fmt.Println("Servidor de alta concurrencia iniciado en http://localhost:8080")
	if err := r.Run(":8080"); err != nil {
		fmt.Printf("Error al iniciar el servicio: %v\n", err)
	}
}
