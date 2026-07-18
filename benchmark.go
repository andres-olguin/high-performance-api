package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type BatchRequest struct {
	Tasks []string `json:"tasks"`
}

func sendRequest(wg *sync.WaitGroup, client *http.Client, taskBatch []string, workerID int) {
	defer wg.Done()

	reqBody, _ := json.Marshal(BatchRequest{Tasks: taskBatch})

	resp, err := client.Post("http://localhost:8080/api/v1/process-batch", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		fmt.Printf("[Worker %d] Error al conectar con el servidor: %v\n", workerID, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("[Worker %d] Lote enviado con exito (Status 200)\n", workerID)
	} else {
		fmt.Printf("[Worker %d] Servidor respondio con error: %d\n", workerID, resp.StatusCode)
	}
}

func main() {
	var wg sync.WaitGroup
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	totalTasks := 1000
	tasksPerRequest := 10 // 100 peticiones de 10 tareas cada una en simultaneo
	numRequests := totalTasks / tasksPerRequest

	fmt.Printf("Iniciando prueba de estres: Enviando %d tareas en %d peticiones concurrentes...\n", totalTasks, numRequests)
	startTime := time.Now()

	for i := 1; i <= numRequests; i++ {
		wg.Add(1)

		var batch []string
		for j := 1; j <= tasksPerRequest; j++ {
			if (i*j)%50 == 0 {
				batch = append(batch, fmt.Sprintf("Tarea_Con_Error_%d_%d", i, j))
			} else {
				batch = append(batch, fmt.Sprintf("Tarea_Masiva_%d_%d", i, j))
			}
		}

		go sendRequest(&wg, client, batch, i)
	}

	wg.Wait()
	fmt.Printf("\n--- PRUEBA FINALIZADA ---\n")
	fmt.Printf("Tiempo total en procesar las 1,000 tareas: %v\n", time.Since(startTime))
}
