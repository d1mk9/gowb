package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	advertsHost         = "https://advert-api.wildberries.ru"
	maxRetries          = 5
	baseRetryDelay      = 5 * time.Second
	maxRetryDelay       = 60 * time.Second
	requestTimeout      = 30 * time.Second
	batchSize           = 50
	requestsPerMinute   = 40
	stateFilename       = "adverts_state.json"
	statsDir            = "adverts_stats"
	progressInterval    = 1 * time.Minute
	delayBetweenBatches = 5 * time.Second
	maxConcurrentReqs   = 2
	dataDir             = "/data"
	httpServerPort      = "8080"
)

type Advert struct {
	AdvertId int `json:"advertId"`
	Type     int `json:"type"`
}

type AppStat struct {
	Views    int     `json:"views"`
	Clicks   int     `json:"clicks"`
	Ctr      float64 `json:"ctr"`
	Cpc      float64 `json:"cpc"`
	Sum      float64 `json:"sum"`
	Atbs     int     `json:"atbs"`
	Orders   int     `json:"orders"`
	Cr       float64 `json:"cr"`
	Shks     int     `json:"shks"`
	SumPrice int     `json:"sum_price"`
	Nm       []struct {
		NmId     int     `json:"nmId"`
		Name     string  `json:"name"`
		Views    int     `json:"views"`
		Clicks   int     `json:"clicks"`
		Ctr      float64 `json:"ctr"`
		Cpc      float64 `json:"cpc"`
		Sum      float64 `json:"sum"`
		Atbs     int     `json:"atbs"`
		Orders   int     `json:"orders"`
		Cr       float64 `json:"cr"`
		Shks     int     `json:"shks"`
		SumPrice int     `json:"sum_price"`
	} `json:"nm"`
	AppType int `json:"appType"`
}

type AdvertStatDay struct {
	Date   string    `json:"date"`
	Views  int       `json:"views"`
	Clicks int       `json:"clicks"`
	Ctr    float64   `json:"ctr"`
	Cpc    float64   `json:"cpc"`
	Sum    float64   `json:"sum"`
	Atbs   int       `json:"atbs"`
	Orders int       `json:"orders"`
	Cr     float64   `json:"cr"`
	Shks   int       `json:"shks"`
	Apps   []AppStat `json:"apps"`
}

type EnhancedAdvertStat struct {
	AdvertId int             `json:"advertId"`
	Type     int             `json:"type"`
	Days     []AdvertStatDay `json:"days"`
}

type ProcessingState struct {
	Date               string               `json:"date"`
	LastProcessedIndex int                  `json:"lastProcessedIndex"`
	AdvertsList        []Advert             `json:"advertsList"`
	ProcessedStats     []EnhancedAdvertStat `json:"processedStats"`
	RetryCount         int                  `json:"retryCount"`
	LastSaved          time.Time            `json:"lastSaved"`
}

type APIError struct {
	Title     string `json:"title"`
	Detail    string `json:"detail"`
	Status    int    `json:"status"`
	Code      string `json:"code"`
	RequestId string `json:"requestId"`
}

type AdvertListItem struct {
	AdvertId   int    `json:"advertId"`
	ChangeTime string `json:"changeTime"`
}

type AdvertGroup struct {
	Type       int              `json:"type"`
	Status     int              `json:"status"`
	Count      int              `json:"count"`
	AdvertList []AdvertListItem `json:"advert_list"`
}

type APIResponse struct {
	Adverts []AdvertGroup `json:"adverts"`
	All     int           `json:"all"`
}

type AdvertInfo struct {
	AdvertId  int    `json:"advertId"`
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
}

type ProductInfo struct {
	Article string `json:"article"`
	Name    string `json:"name"`
}

var (
	requestLimiter     = make(chan struct{}, requestsPerMinute)
	errTooManyRequests = errors.New("too many requests")
	errInvalidToken    = errors.New("invalid or expired token")
	errEmptyResponse   = errors.New("empty response from API")
	retryAfterHeader   = "Retry-After"
	token              string
)

// Добавляем метод Error для соответствия интерфейсу error
func (e *APIError) Error() string {
	return fmt.Sprintf("%s: %s (status: %d, code: %s, requestId: %s)",
		e.Title, e.Detail, e.Status, e.Code, e.RequestId)
}

func init() {
	go func() {
		for range time.Tick(time.Minute / time.Duration(requestsPerMinute)) {
			requestLimiter <- struct{}{}
		}
	}()
}

func main() {
	go startHTTPServer()

	// Бесконечный цикл для периодического выполнения выгрузки
	for {
		// Запускаем выгрузку
		if err := run(); err != nil {
			log.Printf("Application failed: %v", err)
		}

		// Вычисляем время до следующей 6:00 утра по Москве
		nextRun := next6AMMoscow()
		waitDuration := time.Until(nextRun)

		log.Printf("Выгрузка завершена. Следующая выгрузка начнется в %v (через %v)",
			nextRun.Format("2006-01-02 15:04:05"),
			waitDuration.Round(time.Minute))

		// Ждем до 6:00 утра по Москве
		time.Sleep(waitDuration)
	}
}

// next6AMMoscow возвращает следующее время 6:00 утра по московскому времени
func next6AMMoscow() time.Time {
	loc, err := time.LoadLocation("Europe/Moscow")
	if err != nil {
		loc = time.UTC
		log.Printf("Не удалось загрузить таймзону Moscow, используем UTC: %v", err)
	}

	now := time.Now().In(loc)

	// Создаем время 6:00 сегодняшнего дня
	today6AM := time.Date(
		now.Year(),
		now.Month(),
		now.Day(),
		1, 0, 0, 0, // ИЗМЕНИЛ НА 3 УТРА
		loc,
	)

	// Если сейчас уже после 6:00, берем 6:00 следующего дня
	if now.After(today6AM) {
		return today6AM.Add(24 * time.Hour)
	}

	return today6AM
}

func run() error {
	token = os.Getenv("token")
	if err := checkTokenValid(); err != nil {
		return fmt.Errorf("token validation failed: %w", err)
	}

	// Выбираем период для выгрузки - последние 30 дней
	dateFrom := time.Now().AddDate(0, 0, -30).Truncate(24 * time.Hour)
	dateTo := time.Now().Truncate(24 * time.Hour)

	result, err := loadAdvertsDataByDate(dateFrom, dateTo)
	if err != nil {
		return fmt.Errorf("failed to load adverts data: %w", err)
	}

	log.Printf("Выгрузка успешно завершена. Обработано %d/%d рекламных кампаний",
		result["processed"], result["total"])
	return nil
}

func startHTTPServer() {
	http.HandleFunc("/download", downloadHandler)
	http.HandleFunc("/status", statusHandler)
	http.Handle("/files/", http.StripPrefix("/files/", http.FileServer(http.Dir(dataDir))))

	log.Printf("Starting HTTP server on port %s", httpServerPort)
	if err := http.ListenAndServe(":"+httpServerPort, nil); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	filename := r.URL.Query().Get("filename")
	if filename == "" {
		http.Error(w, "Filename parameter is required", http.StatusBadRequest)
		return
	}

	filePath := filepath.Join(dataDir, filepath.Clean(filename))

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	file, err := os.Open(filePath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open file: %v", err), http.StatusInternalServerError)
		return
	}
	defer file.Close()

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filepath.Base(filePath)))
	w.Header().Set("Content-Type", "application/octet-stream")

	if _, err := io.Copy(w, file); err != nil {
		http.Error(w, fmt.Sprintf("Failed to send file: %v", err), http.StatusInternalServerError)
		return
	}
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	state, err := loadState()
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "No active processing", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("Failed to load state: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"status":          "in progress",
		"processed":       state.LastProcessedIndex,
		"total":           len(state.AdvertsList),
		"last_saved":      state.LastSaved,
		"processed_stats": len(state.ProcessedStats),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
	}
}

func getProductName(article string) (string, error) {
	nmId, err := strconv.Atoi(article)
	if err != nil {
		return "", fmt.Errorf("invalid article format: %w", err)
	}

	url := fmt.Sprintf("https://card.wildberries.ru/cards/detail?nm=%d", nmId)
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to get product info: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Data struct {
			Products []struct {
				Name string `json:"name"`
			} `json:"products"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	if len(result.Data.Products) == 0 {
		return "", errors.New("product not found")
	}

	return result.Data.Products[0].Name, nil
}

func saveAdvertsStats(stats []EnhancedAdvertStat) error {
	statsDirPath := getStatsDir()
	if err := os.MkdirAll(statsDirPath, 0755); err != nil {
		return fmt.Errorf("не удалось создать директорию %s: %v", statsDirPath, err)
	}

	if err := checkWritePermissions(statsDirPath); err != nil {
		return err
	}

	if len(stats) == 0 {
		return errors.New("нет данных для сохранения")
	}

	productInfo, err := getProductsInfo(stats)
	if err != nil {
		log.Printf("Не удалось получить информацию о товарах: %v", err)
		productInfo = make(map[int]ProductInfo)
	}

	csvFile := filepath.Join(statsDirPath, "stats.csv")
	if err := createStatsCSV(csvFile, stats, productInfo); err != nil {
		return err
	}

	log.Printf("Данные успешно сохранены в CSV: %s", csvFile)
	return nil
}

func checkWritePermissions(dir string) error {
	testFile := filepath.Join(dir, "test_write.tmp")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		return fmt.Errorf("нет прав на запись в %s: %v", dir, err)
	}
	os.Remove(testFile)
	return nil
}

func getProductsInfo(stats []EnhancedAdvertStat) (map[int]ProductInfo, error) {
	products := make(map[int]ProductInfo)

	// Сначала попробуем получить информацию из API рекламных кампаний
	wbArticles, err := getWBArticlesForStats(stats)
	if err != nil {
		log.Printf("Ошибка получения артикулов WB: %v", err)
		wbArticles = make(map[int]string)
	}

	// Затем попробуем извлечь информацию из самих статистических данных
	for _, stat := range stats {
		if _, ok := products[stat.AdvertId]; !ok {
			// Проверяем, есть ли информация в статистике
			if len(stat.Days) > 0 && len(stat.Days[0].Apps) > 0 && len(stat.Days[0].Apps[0].Nm) > 0 {
				nm := stat.Days[0].Apps[0].Nm[0]
				products[stat.AdvertId] = ProductInfo{
					Article: strconv.Itoa(nm.NmId),
					Name:    nm.Name,
				}
			} else if article, ok := wbArticles[stat.AdvertId]; ok && article != "N/A" {
				// Если в статистике нет, но есть в wbArticles
				name, err := getProductName(article)
				if err != nil {
					log.Printf("Не удалось получить название для артикула %s: %v", article, err)
					products[stat.AdvertId] = ProductInfo{
						Article: article,
						Name:    "Неизвестный товар",
					}
				} else {
					products[stat.AdvertId] = ProductInfo{
						Article: article,
						Name:    name,
					}
				}
			} else {
				products[stat.AdvertId] = ProductInfo{
					Article: "N/A",
					Name:    "Неизвестный товар",
				}
			}
		}
	}

	return products, nil
}

func getProductNamesBatch(nmIDs []string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Преобразуем строковые nmID в числовые
	var nmIDsInt []int
	for _, nmID := range nmIDs {
		if nmID == "N/A" {
			continue
		}
		id, err := strconv.Atoi(nmID)
		if err != nil {
			continue
		}
		nmIDsInt = append(nmIDsInt, id)
	}

	if len(nmIDsInt) == 0 {
		return make(map[string]string), nil
	}

	url := "https://card.wildberries.ru/cards/detail?nm=" + strings.Join(convertToStringSlice(nmIDsInt), ";")

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API returned error: %d - %s", resp.StatusCode, string(body))
	}

	var response struct {
		Data struct {
			Products []struct {
				Id    int    `json:"id"`
				Name  string `json:"name"`
				Brand string `json:"brand"`
			} `json:"products"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	result := make(map[string]string)
	for _, product := range response.Data.Products {
		result[strconv.Itoa(product.Id)] = product.Name
	}

	return result, nil
}

func convertToStringSlice(nums []int) []string {
	result := make([]string, len(nums))
	for i, num := range nums {
		result[i] = strconv.Itoa(num)
	}
	return result
}

func createStatsCSV(filename string, stats []EnhancedAdvertStat, products map[int]ProductInfo) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("ошибка создания CSV файла: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{
		"ID рк", "Тип рк", "Артикул WB", "Название",
		"Дата", "Просмотры", "Клики", "Заказы",
		"Сумма трат", "CPC", "CTR",
	}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("ошибка записи заголовков CSV: %v", err)
	}

	for _, stat := range stats {
		product, ok := products[stat.AdvertId]
		if !ok {
			product = ProductInfo{
				Article: "N/A",
				Name:    "Неизвестный товар",
			}
		}

		for _, day := range stat.Days {
			date := day.Date[:10]

			cleanName := cleanProductName(product.Name)

			record := []string{
				strconv.Itoa(stat.AdvertId),
				strconv.Itoa(stat.Type),
				product.Article,
				cleanName,
				date,
				strconv.Itoa(day.Views),
				strconv.Itoa(day.Clicks),
				strconv.Itoa(day.Shks),
				fmt.Sprintf("%.2f", day.Sum),
				fmt.Sprintf("%.2f", day.Cpc),
				fmt.Sprintf("%.2f", day.Ctr),
			}

			if err := writer.Write(record); err != nil {
				return fmt.Errorf("ошибка записи строки CSV: %v", err)
			}
		}
	}
	return nil
}

func cleanProductName(name string) string {
	name = regexp.MustCompile(`Поиск от \d{2}\.\d{2}\.\d{4}$`).ReplaceAllString(name, "")
	name = regexp.MustCompile(`от \d{2}\.\d{2}\.\d{4}$`).ReplaceAllString(name, "")
	name = strings.ReplaceAll(name, "Поиск", "")
	return strings.TrimSpace(name)
}

func getWBArticlesForStats(stats []EnhancedAdvertStat) (map[int]string, error) {
	var advertIds []int
	for _, stat := range stats {
		advertIds = append(advertIds, stat.AdvertId)
	}

	wbArticles := make(map[int]string)
	for i := 0; i < len(advertIds); i += 50 {
		end := i + 50
		if end > len(advertIds) {
			end = len(advertIds)
		}

		batch := advertIds[i:end]
		batchArticles, err := getWBArticlesBatch(batch)
		if err != nil {
			return nil, err
		}

		for id, article := range batchArticles {
			wbArticles[id] = article
		}
	}

	return wbArticles, nil
}

func getWBArticlesBatch(advertIds []int) (map[int]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	url := advertsHost + "/adv/v1/promotion/adverts"

	payload, err := json.Marshal(advertIds)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API returned error: %d - %s", resp.StatusCode, string(body))
	}

	var advertsInfo []struct {
		AdvertId int    `json:"advertId"`
		NmId     int    `json:"nmId"`
		Name     string `json:"name"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&advertsInfo); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	result := make(map[int]string)
	for _, advert := range advertsInfo {
		if advert.NmId != 0 {
			result[advert.AdvertId] = strconv.Itoa(advert.NmId)
		} else {
			// Если nmId отсутствует, попробуем извлечь его из имени (на случай, если API изменилось)
			if advert.Name != "" {
				if nmId := extractNmIdFromName(advert.Name); nmId != "" {
					result[advert.AdvertId] = nmId
				} else {
					result[advert.AdvertId] = "N/A"
				}
			} else {
				result[advert.AdvertId] = "N/A"
			}
		}
	}

	return result, nil
}

func extractNmIdFromName(name string) string {
	// Пытаемся найти nmId в названии (например: "Товар (12345678)")
	re := regexp.MustCompile(`\((\d+)\)`)
	matches := re.FindStringSubmatch(name)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}

// Остальные функции остаются без изменений
func loadAdvertsDataByDate(dateFrom, dateTo time.Time) (map[string]interface{}, error) {
	state, err := loadOrInitState(dateFrom, dateTo)
	if err != nil {
		return nil, fmt.Errorf("failed to init state: %w", err)
	}

	result := map[string]interface{}{
		"status":    "in progress",
		"processed": state.LastProcessedIndex,
		"total":     len(state.AdvertsList),
	}

	for {
		processedInThisRound := 0
		concurrencyLimit := make(chan struct{}, 3)
		var wg sync.WaitGroup
		var mu sync.Mutex

		for i := state.LastProcessedIndex; i < len(state.AdvertsList); i++ {
			wg.Add(1)
			concurrencyLimit <- struct{}{}

			go func(idx int, advert Advert) {
				defer func() {
					<-concurrencyLimit
					wg.Done()
				}()

				stat, err := getAdvertStatWithRetry(advert, dateFrom, dateTo)
				if err != nil {
					if errors.Is(err, errEmptyResponse) {
						log.Printf("No data for advert %d in period %s - %s",
							advert.AdvertId,
							dateFrom.Format("2006-01-02"),
							dateTo.Format("2006-01-02"))
						mu.Lock()
						state.LastProcessedIndex = idx + 1
						processedInThisRound++
						result["processed"] = state.LastProcessedIndex
						mu.Unlock()
						return
					}
					log.Printf("Failed to get stats for advert %d: %v (will retry)", advert.AdvertId, err)
					return
				}

				mu.Lock()
				state.ProcessedStats = append(state.ProcessedStats, *stat)
				state.LastProcessedIndex = idx + 1
				processedInThisRound++
				result["processed"] = state.LastProcessedIndex
				mu.Unlock()

				if time.Since(state.LastSaved) > progressInterval {
					if err := saveState(state); err != nil {
						log.Printf("Failed to save state: %v", err)
					} else {
						state.LastSaved = time.Now()
					}
				}
			}(i, state.AdvertsList[i])
		}

		wg.Wait()

		if state.LastProcessedIndex >= len(state.AdvertsList) {
			break
		}

		log.Printf("Completed round with %d processed, %d remaining. Starting new round...",
			processedInThisRound,
			len(state.AdvertsList)-state.LastProcessedIndex)

		time.Sleep(30 * time.Second)
	}

	if err := saveAdvertsStats(state.ProcessedStats); err != nil {
		return result, fmt.Errorf("failed to save stats: %w", err)
	}

	if err := resetState(); err != nil {
		log.Printf("Failed to reset state: %v", err)
	}

	result["status"] = "complete"
	return result, nil
}

func loadOrInitState(dateFrom, dateTo time.Time) (*ProcessingState, error) {
	state, err := loadState()
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load state: %w", err)
	}

	if state != nil && state.Date == dateFrom.Format(time.RFC3339) && len(state.AdvertsList) > 0 {
		log.Printf("Reusing existing state for date %s with %d adverts", state.Date, len(state.AdvertsList))
		return state, nil
	}

	log.Printf("Initializing new state for period %s - %s",
		dateFrom.Format("2006-01-02"),
		dateTo.Format("2006-01-02"))

	advertsList, err := getCachedAdvertsList(dateFrom, dateTo)
	if err != nil {
		return nil, fmt.Errorf("failed to get adverts list for period %s-%s: %w",
			dateFrom.Format("2006-01-02"),
			dateTo.Format("2006-01-02"),
			err)
	}

	actualAdverts, err := getActualAdvertsList(advertsList, dateFrom, dateTo)
	if err != nil {
		return nil, fmt.Errorf("failed to filter adverts for period %s-%s: %w",
			dateFrom.Format("2006-01-02"),
			dateTo.Format("2006-01-02"),
			err)
	}

	if len(actualAdverts) == 0 {
		log.Printf("Warning: no active adverts found for period %s - %s",
			dateFrom.Format("2006-01-02"),
			dateTo.Format("2006-01-02"))
	}

	newState := &ProcessingState{
		Date:               dateFrom.Format(time.RFC3339),
		AdvertsList:        actualAdverts,
		LastProcessedIndex: 0,
		ProcessedStats:     []EnhancedAdvertStat{},
		RetryCount:         0,
		LastSaved:          time.Now(),
	}

	if err := saveState(newState); err != nil {
		log.Printf("Warning: failed to save initial state: %v", err)
	}

	log.Printf("Initialized new state with %d adverts", len(actualAdverts))
	return newState, nil
}

func getAdvertStatWithRetry(advert Advert, dateFrom, dateTo time.Time) (*EnhancedAdvertStat, error) {
	const maxRetries = 5
	var lastError error
	var lastStatusCode int

	for retry := 0; retry < maxRetries; retry++ {
		<-requestLimiter

		stats, err := getAdvertStat(advert.AdvertId, dateFrom, dateTo)
		if err == nil {
			return &EnhancedAdvertStat{
				AdvertId: advert.AdvertId,
				Type:     advert.Type,
				Days:     stats,
			}, nil
		}

		// Сохраняем статус код последней ошибки
		if apiErr, ok := err.(*APIError); ok {
			lastStatusCode = apiErr.Status
		}

		// Если это пустой ответ - логируем и пропускаем кампанию
		if errors.Is(err, errEmptyResponse) {
			log.Printf("Пустой ответ для кампании %d. Пропускаем.", advert.AdvertId)
			return nil, errEmptyResponse
		}

		// Особенная обработка ошибки "too many requests"
		if errors.Is(err, errTooManyRequests) {
			waitTime := calculateWaitTime(retry)
			log.Printf("Превышен лимит запросов для кампании %d. Ожидание %v перед повторной попыткой %d/%d",
				advert.AdvertId, waitTime, retry+1, maxRetries)
			time.Sleep(waitTime)
			lastError = err
			continue
		}

		// Обработка nil ошибки
		if err == nil {
			err = fmt.Errorf("неизвестная ошибка (nil)")
		}

		lastError = err
		waitTime := calculateBackoff(retry)
		log.Printf("Повторная попытка %d/%d для кампании %d через %v. Ошибка: %v (Статус: %d)",
			retry+1, maxRetries, advert.AdvertId, waitTime, err, lastStatusCode)
		time.Sleep(waitTime)
	}

	// Если последняя ошибка была nil, создаем понятное сообщение
	if lastError == nil {
		lastError = fmt.Errorf("неизвестная ошибка после %d попыток", maxRetries)
	}

	return nil, fmt.Errorf("после %d попыток не удалось получить статистику для кампании %d: %v (Последний статус: %d)",
		maxRetries, advert.AdvertId, lastError, lastStatusCode)
}

func calculateWaitTime(retry int) time.Duration {
	baseWait := 30 * time.Second
	waitTime := baseWait * time.Duration(retry+1)

	// Максимальное время ожидания - 5 минут
	if waitTime > 5*time.Minute {
		waitTime = 5 * time.Minute
	}
	return waitTime
}

func getAdvertStat(advertId int, dateFrom, dateTo time.Time) ([]AdvertStatDay, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	url := advertsHost + "/adv/v2/fullstats"
	interval := map[string]string{
		"begin": dateFrom.Format("2006-01-02"),
		"end":   dateTo.Format("2006-01-02"),
	}

	payload := []map[string]interface{}{
		{
			"id":       advertId,
			"interval": interval,
		},
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("ошибка формирования запроса: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, fmt.Errorf("ошибка создания запроса: %v", err)
	}

	setHeaders(req)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ошибка при отправке запроса: %v", err)
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ошибка чтения ответа: %v", err)
	}

	// Логируем ответ API для диагностики
	log.Printf("Ответ API для кампании %d: статус %d, тело: %s", advertId, resp.StatusCode, string(body))

	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := calculateRetryAfter(resp)
		log.Printf("Превышен лимит запросов. API требует ожидания %v", retryAfter)
		time.Sleep(retryAfter)
		return nil, errTooManyRequests
	}

	if resp.StatusCode != http.StatusOK {
		var apiErr APIError
		if err := json.Unmarshal(body, &apiErr); err == nil {
			return nil, &apiErr
		}
		return nil, fmt.Errorf("ошибка API: %d - %s", resp.StatusCode, string(body))
	}

	if len(body) == 0 || string(body) == "null" {
		return nil, errEmptyResponse
	}

	var statResp []struct {
		AdvertId int             `json:"advertId"`
		Days     []AdvertStatDay `json:"days"`
	}

	if err := json.Unmarshal(body, &statResp); err != nil {
		return nil, fmt.Errorf("ошибка разбора ответа: %v. Тело ответа: %s", err, string(body))
	}

	if len(statResp) == 0 {
		return nil, fmt.Errorf("пустой массив в ответе")
	}

	if len(statResp[0].Days) == 0 {
		return nil, errEmptyResponse
	}

	return statResp[0].Days, nil
}

func calculateRetryAfter(resp *http.Response) time.Duration {
	defaultWait := 30 * time.Second
	if retryHeader := resp.Header.Get(retryAfterHeader); retryHeader != "" {
		if seconds, err := strconv.Atoi(retryHeader); err == nil {
			return time.Duration(seconds) * time.Second
		}
	}
	return defaultWait
}

func getAdvertsList(dateFrom, dateTo time.Time) ([]Advert, error) {
	// Убираем параметры фильтрации из URL
	url := fmt.Sprintf("%s/adv/v1/promotion/count", advertsHost)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	setHeaders(req)

	// Добавляем query-параметры дат
	q := req.URL.Query()
	q.Add("dateFrom", dateFrom.Format("2006-01-02"))
	q.Add("dateTo", dateTo.Format("2006-01-02"))
	req.URL.RawQuery = q.Encode()

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	var adverts []Advert
	for _, group := range apiResp.Adverts {
		// Включаем все типы кампаний
		for _, item := range group.AdvertList {
			adverts = append(adverts, Advert{
				AdvertId: item.AdvertId,
				Type:     group.Type,
			})
		}
	}

	log.Printf("Total adverts loaded: %d (all types and statuses)", len(adverts))
	return adverts, nil
}

func getActualAdvertsList(fullList []Advert, dateFrom, dateTo time.Time) ([]Advert, error) {
	var actualAdverts []Advert
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrentReqs)

	for i := 0; i < len(fullList); i += batchSize {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			sem <- struct{}{}

			end := start + batchSize
			if end > len(fullList) {
				end = len(fullList)
			}
			batch := fullList[start:end]

			ids := make([]int, len(batch))
			for i, adv := range batch {
				ids[i] = adv.AdvertId
			}

			advertsInfo, err := getAdvertsInfoBatch(ids)
			if err != nil {
				log.Printf("Batch error: %v", err)
				<-sem
				return
			}

			var filtered []Advert
			for _, info := range advertsInfo {
				// Включаем ВСЕ кампании, которые пересекаются с периодом
				if isAdvertActual(info, dateFrom, dateTo) {
					for _, adv := range batch {
						if adv.AdvertId == info.AdvertId {
							filtered = append(filtered, adv)
							break
						}
					}
				}
			}

			mu.Lock()
			actualAdverts = append(actualAdverts, filtered...)
			mu.Unlock()

			<-sem
		}(i)
	}

	wg.Wait()
	return actualAdverts, nil
}

func getCachedAdvertsList(dateFrom, dateTo time.Time) ([]Advert, error) {
	cacheFile := getCachePath(dateFrom, dateTo)

	if data, err := os.ReadFile(cacheFile); err == nil {
		var adverts []Advert
		if err := json.Unmarshal(data, &adverts); err == nil {
			log.Printf("Using cached adverts list for %s - %s (%d items)",
				dateFrom.Format("2006-01-02"),
				dateTo.Format("2006-01-02"),
				len(adverts))
			return adverts, nil
		}
	}

	adverts, err := getAdvertsList(dateFrom, dateTo)
	if err != nil {
		return nil, err
	}

	data, err := json.Marshal(adverts)
	if err != nil {
		return adverts, nil
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Printf("Failed to create data directory: %v", err)
		return adverts, nil
	}

	if err := atomicWriteFile(cacheFile, data); err != nil {
		log.Printf("Failed to save cache: %v", err)
	}

	return adverts, nil
}

func getAdvertsInfoBatch(advertIds []int) ([]AdvertInfo, error) {
	const maxRetries = 3
	var lastError error

	for attempt := 0; attempt < maxRetries; attempt++ {
		url := advertsHost + "/adv/v1/promotion/adverts"

		payload, err := json.Marshal(advertIds)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload for batch %v: %w", advertIds, err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(payload))
		if err != nil {
			return nil, fmt.Errorf("failed to create request for batch %v: %w", advertIds, err)
		}

		setHeaders(req)

		log.Printf("Requesting info for batch of %d adverts (attempt %d/%d)",
			len(advertIds), attempt+1, maxRetries)

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			lastError = fmt.Errorf("request failed for batch %v: %w", advertIds, err)
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		defer func() {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			lastError = fmt.Errorf("failed to read response for batch %v: %w", advertIds, err)
			continue
		}

		switch resp.StatusCode {
		case http.StatusOK:
			var advertsInfo []AdvertInfo
			if err := json.Unmarshal(body, &advertsInfo); err != nil {
				return nil, fmt.Errorf("failed to parse response for batch %v: %w (body: %s)",
					advertIds, err, string(body))
			}
			return advertsInfo, nil

		case http.StatusTooManyRequests:
			retryAfter := 5
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if raInt, err := strconv.Atoi(ra); err == nil {
					retryAfter = raInt
				}
			}
			log.Printf("Rate limit hit, retrying after %d seconds", retryAfter)
			time.Sleep(time.Duration(retryAfter) * time.Second)
			lastError = fmt.Errorf("rate limit exceeded for batch %v", advertIds)
			continue

		default:
			lastError = fmt.Errorf("unexpected status code %d for batch %v: %s",
				resp.StatusCode, advertIds, string(body))
			continue
		}
	}

	return nil, fmt.Errorf("after %d attempts, last error: %w", maxRetries, lastError)
}

func isAdvertActual(info AdvertInfo, dateFrom, dateTo time.Time) bool {
	// Проверяем только пересечение периодов, без учета статуса/типа
	startDate, err := time.Parse(time.RFC3339, info.StartTime)
	if err != nil {
		log.Printf("Error parsing start date: %v", err)
		return false
	}

	endDate, err := time.Parse(time.RFC3339, info.EndTime)
	if err != nil {
		log.Printf("Error parsing end date: %v", err)
		return true // Если дата окончания некорректна, все равно включаем кампанию
	}

	return !endDate.Before(dateFrom) && !startDate.After(dateTo)
}

func loadState() (*ProcessingState, error) {
	var state ProcessingState
	statePath := getStatePath()

	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &state, nil
		}
		return nil, err
	}

	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

func saveState(state *ProcessingState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("ошибка сериализации: %v", err)
	}

	log.Printf("Сохранение состояния: %d/%d кампаний обработано",
		state.LastProcessedIndex, len(state.AdvertsList))

	statePath := getStatePath()
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("ошибка создания директории: %v", err)
	}

	if err := atomicWriteFile(statePath, data); err != nil {
		return fmt.Errorf("ошибка записи файла: %v", err)
	}

	return nil
}

func resetState() error {
	return os.Remove(getStatePath())
}

func checkTokenValid() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	url := advertsHost + "/adv/v1/promotion/count"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	setHeaders(req)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	log.Printf("Token check response status: %d, body: %s", resp.StatusCode, string(body))

	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("invalid token (HTTP 401). Response: %s", string(body))
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d. Response: %s", resp.StatusCode, string(body))
	}

	return nil
}

func calculateBackoff(retry int) time.Duration {
	delay := time.Duration(math.Pow(2, float64(retry))) * baseRetryDelay
	if delay > maxRetryDelay {
		return maxRetryDelay
	}
	return delay
}

func shouldRetry(err error) bool {
	return !errors.Is(err, errInvalidToken)
}

func setHeaders(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "WB-Stats-Collector/1.0")
}

func getStatePath() string {
	return filepath.Join(dataDir, stateFilename)
}

func getCachePath(dateFrom, dateTo time.Time) string {
	return filepath.Join(dataDir, fmt.Sprintf("adverts_cache_%s_%s.json",
		dateFrom.Format("20060102"),
		dateTo.Format("20060102")))
}

func getStatsDir() string {
	return filepath.Join(dataDir, statsDir)
}

func atomicWriteFile(filename string, data []byte) error {
	tmpFilename := filename + ".tmp"
	if err := os.WriteFile(tmpFilename, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}
	return os.Rename(tmpFilename, filename)
}
