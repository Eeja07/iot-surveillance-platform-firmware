#include <WiFi.h>
#include <WiFiManager.h>
#include <ESPmDNS.h>
#include <ArduinoWebsockets.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include "esp_camera.h"
#include "base64.h"
#include <deque> 
#include <esp_system.h>
#include <WiFiClientSecure.h>
#include "esp_heap_caps.h"
#include <HTTPClient.h>
#include <Update.h>
#include "mbedtls/sha256.h"
#include <esp_partition.h>
#include <esp_ota_ops.h>
#include <esp_task_wdt.h>
#include <freertos/event_groups.h>

struct PublishRequest {
    char topic[96];
    char payload[3072];
    bool retained;
};

#define LED_PIN 4
#define DEVICE_HOSTNAME "cctv-medium-local"
#define FW_VERSION "1.0.3"
#define FW_BOARD   "ESP32-CAM"
#define FW_MODEL   "AI_THINKER"
#define FW_BUILD   __DATE__ " " __TIME__
#define BUTTON_PIN 14

unsigned long buttonPressStart = 0;
const char* mqtt_broker   = "xxx"; 
const int   mqtt_port     = 443;           
const char* mqtt_path     = "/mqtt";
const char* mqtt_user     = "xxx";      
const char* mqtt_pass     = "xxx";   
const char* device_id     = "xxx"; 

const String topic_image      = "ws/camera/" + String(device_id) + "/image";
const String topic_telemetry  = "ws/camera/" + String(device_id) + "/telemetry";
const String topic_status     = "ws/camera/" + String(device_id) + "/status";
const String topic_ota        = "ws/camera/" + String(device_id) + "/ota";
const String topic_ota_status = "ws/camera/" + String(device_id) + "/ota/status";
const String topic_config        = "ws/camera/" + String(device_id) + "/config";
const String topic_config_status = "ws/camera/" + String(device_id) + "/config/status";

// Remote Device Configuration variables
bool image_enabled = true;
bool telemetry_enabled = true;
bool ota_enabled = true;
framesize_t current_frame_size = FRAMESIZE_VGA;
int current_jpeg_quality = 20;

#define PWDN_GPIO_NUM     32
#define RESET_GPIO_NUM    -1
#define XCLK_GPIO_NUM      0
#define SIOD_GPIO_NUM     26
#define SIOC_GPIO_NUM     27
#define Y9_GPIO_NUM       35
#define Y8_GPIO_NUM       34
#define Y7_GPIO_NUM       39
#define Y6_GPIO_NUM       36
#define Y5_GPIO_NUM       21
#define Y4_GPIO_NUM       19
#define Y3_GPIO_NUM       18
#define Y2_GPIO_NUM        5
#define VSYNC_GPIO_NUM    25
#define HREF_GPIO_NUM     23
#define PCLK_GPIO_NUM     22

struct SHA256Hasher {
    mbedtls_sha256_context ctx;
    SHA256Hasher() {
        mbedtls_sha256_init(&ctx);
        mbedtls_sha256_starts(&ctx, 0);
    }
    ~SHA256Hasher() {
        mbedtls_sha256_free(&ctx);
    }
    void update(const uint8_t* data, size_t len) {
        mbedtls_sha256_update(&ctx, (const unsigned char*)data, len);
    }
    String finish() {
        unsigned char hash[32];
        mbedtls_sha256_finish(&ctx, hash);
        char hex[65];
        for (int i = 0; i < 32; i++) {
            sprintf(hex + (i * 2), "%02x", hash[i]);
        }
        hex[64] = '\0';
        return String(hex);
    }
};

using namespace websockets;
WebsocketsClient wsClient;
extern bool realWsConnected;

class WebsocketClientWrapper : public Client {
private:
    std::deque<uint8_t> _rxBuffer;
public:
    uint8_t connected()
    {
        return realWsConnected;
    }
    size_t write(uint8_t b) { return write(&b, 1); }
    size_t write(
        const uint8_t *buf,
        size_t size
    )
    {
        bool ok = wsClient.sendBinary(
            (const char*)buf,
            size
        );

        return ok ? size : 0;
    }
    int available()
    {
        wsClient.poll();
        return _rxBuffer.size();
    }
    int read() { 
        if (_rxBuffer.empty()) return -1;
        uint8_t b = _rxBuffer.front(); _rxBuffer.pop_front();
        return b;
    }
    int read(uint8_t *buf, size_t size) {
        int toRead = std::min((size_t)available(), size);
        for(int i=0; i<toRead; i++) buf[i] = read();
        return toRead;
    }
    void pushData(const uint8_t* data, size_t size) {
        for(size_t i=0; i<size; i++) _rxBuffer.push_back(data[i]);
    }
    int peek() { return _rxBuffer.empty() ? -1 : _rxBuffer.front(); }
    void flush() {}
    void stop() { wsClient.close(); _rxBuffer.clear(); }
    operator bool()
    {
        return true;
    } 
    int connect(IPAddress ip, uint16_t port) { return 1; }
    int connect(const char *host, uint16_t port) { return 1; }
};

WebsocketClientWrapper wsWrapper;
PubSubClient mqttClient(wsWrapper);

unsigned long last_capture_time = 0;
unsigned long capture_interval_ms = 3000; 

unsigned long last_telemetry_time = 0;
unsigned long telemetry_interval_ms = 60000;
String lastRecoveryReason = "NONE";
volatile uint32_t publisherProgressMillis = 0;
volatile bool publisherStarted = false;
volatile bool remoteRestartRequested = false;
volatile bool remoteCameraReinitRequested = false;
// Edit 2 & 3: Event group for safe multi-core OTA synchronization
EventGroupHandle_t otaSyncEventGroup = nullptr;
#define CAMERA_CAPTURE_IDLE_BIT (1 << 0)
#define PUBLISHER_IDLE_BIT      (1 << 1)
uint32_t captureOkCount = 0;
uint32_t captureFailCount = 0;
uint32_t fbNullCount = 0;
uint32_t publishOkCount = 0;
uint32_t publishFailCount = 0;
uint32_t cameraResetCount = 0;
uint32_t lastCaptureMs = 0;
uint32_t maxCaptureMs = 0;
uint32_t publishMs = 0;
uint32_t maxPublishMs = 0;
uint32_t frameSize = 0;
uint32_t maxFrameSize = 0;
uint32_t base64Size = 0;
uint32_t maxBase64Size = 0;
uint32_t mqttBufferSize = 0;
uint32_t wifiDropCount = 0;
uint32_t mqttReconnectCount = 0;
bool firstMqttConnect = true;
bool realWsConnected = false;
unsigned long lastMqttConnectMillis = 0;
char lastDisconnectReason[48] = "NONE";
uint32_t publishStallCount = 0;
uint32_t transportRecoveryCount = 0;
uint32_t loopCounter = 0;
unsigned long lastCameraRecovery = 0;

// Surgical stability variables
uint32_t consecutive_failures = 0;
bool skip_next_frame = false;
unsigned long suspend_capture_until = 0;
bool transport_slow = false;
uint32_t camera_consecutive_fails = 0;
double average_jpeg_size = 0.0;
uint32_t maximum_jpeg_size = 0;

// Timing instrumentation variables
uint32_t lastEncodeMs = 0;
uint32_t mqtt_connect_ms = 0;
uint32_t wifi_connect_ms = 0;
uint32_t camera_init_ms = 0;

// Recovery variables
unsigned long last_failure_free_time = 0;
unsigned long recovery_start_time = 0;
unsigned long last_publish_fail_time = 0;
const char* recovery_state = "NORMAL";
uint32_t connect_dns_ms = 0;
uint32_t connect_ws_ms = 0;
uint32_t connect_mqtt_ms = 0;
unsigned long mqtt_unavailable_start = 0;

// Zero-allocation buffer
char* base64_buffer = nullptr;
size_t base64_buffer_size = 128 * 1024;

// Non-blocking OTA State Machine variables
int otaRetryCount = 0;
int otaBackoffMs = 1000;
unsigned long otaWaitStart = 0;
bool otaInWait = false;
WiFiClientSecure otaClient;
HTTPClient otaHttp;
String otaManifestContent = "";
SHA256Hasher* otaHasher = nullptr;
size_t otaWritten = 0;
int otaLastProgress = 0;
unsigned long otaLastHeartbeatTime = 0;
unsigned long otaLastDataMillis = 0;

uint32_t lastCaptureSuccessMillis = 0;
const uint32_t DEADMAN_TIMEOUT_MS = 300000UL; // 5 menit
const uint32_t TRANSPORT_WATCHDOG_TIMEOUT_MS = 180000UL; // 3 menit
const uint32_t MAX_TRANSPORT_RECOVERY = 10;

// Transport health watchdog and diagnostics
const char* const STATE_CONNECTED = "CONNECTED";
const char* const STATE_DISCONNECTED = "DISCONNECTED";
const char* const STATE_CONNECT_FAILED = "CONNECT_FAILED";
const char* const STATE_RECOVERING = "RECOVERING";

volatile uint32_t lastPublishSuccessMillis = 0;
uint32_t publishQueueOverflowCount = 0;
uint32_t publishQueueDropCount = 0;
uint32_t publishQueuePeak = 0;
volatile uint32_t lastWatchdogRecoveryMillis = 0;
const char* volatile transport_state = STATE_DISCONNECTED;
volatile uint32_t consecutive_transport_recovery = 0;
volatile bool last_publish_result = false;
volatile bool last_connect_result = false;
volatile int lastFailedPublishMqttState = 0;
volatile wl_status_t lastFailedPublishWifiStatus = WL_IDLE_STATUS;
volatile uint32_t watchdog_trigger_count = 0;
const char* volatile watchdog_last_reason = "NONE";
volatile uint32_t last_watchdog_trigger_ms = 0;
volatile uint32_t last_transport_recovery_ms = 0;
volatile int transport_recovery_level = 0;

enum OTAState {
    OTA_IDLE,
    OTA_DOWNLOAD_MANIFEST,
    OTA_VALIDATE,
    OTA_DOWNLOAD_FIRMWARE,
    OTA_VERIFY,
    OTA_FLASH,
    OTA_SUCCESS,
    OTA_FAILED,
    OTA_CANCELLED
};

enum RecoveryLevel {
    REC_NONE = 0,
    REC_MQTT_RECONNECT = 1,
    REC_TRANSPORT_RECONNECT = 2,
    REC_WIFI_RECONNECT = 3,
    REC_WIFI_BEGIN = 4,
    REC_REBOOT = 5
};

enum class RecoveryReason {
    NONE = 0,
    WATCHDOG_TIMEOUT = 1,
    CONSECUTIVE_FAILURES = 2,
    MQTT_CONNECTING = 3,
    WIFI_OFFLINE_300S = 4,
    WIFI_OFFLINE_600S = 5,
    CONSECUTIVE_TRANSPORT_FAILURES = 6,
    DEADMAN_TIMEOUT = 7,
    PUBLISHER_STUCK = 8
};

enum TransportHealth {
    NORMAL,
    WARNING,
    RECOVERY_REQUIRED
};

inline const char* getRecoveryReasonStr(RecoveryReason reason) {
    switch (reason) {
        case RecoveryReason::NONE: return "NONE";
        case RecoveryReason::WATCHDOG_TIMEOUT: return "TIMEOUT";
        case RecoveryReason::CONSECUTIVE_FAILURES: return "CONSECUTIVE_FAILURES";
        case RecoveryReason::MQTT_CONNECTING: return "MQTT_CONNECTING";
        case RecoveryReason::WIFI_OFFLINE_300S: return "WIFI_OFFLINE_300S";
        case RecoveryReason::WIFI_OFFLINE_600S: return "WIFI_OFFLINE_600S";
        case RecoveryReason::CONSECUTIVE_TRANSPORT_FAILURES: return "CONSECUTIVE_TRANSPORT_FAILURES";
        case RecoveryReason::DEADMAN_TIMEOUT: return "DEADMAN_TIMEOUT";
        case RecoveryReason::PUBLISHER_STUCK: return "PUBLISHER_STUCK";
        default: return "UNKNOWN";
    }
}

void updateTransportState(const char* state, RecoveryLevel level, RecoveryReason reason) {
    transport_state = state;
    transport_recovery_level = (int)level;
    if (level != REC_NONE) {
        last_transport_recovery_ms = millis();
        if (reason == RecoveryReason::WATCHDOG_TIMEOUT || reason == RecoveryReason::CONSECUTIVE_FAILURES) {
            watchdog_trigger_count++;
            watchdog_last_reason = getRecoveryReasonStr(reason);
            last_watchdog_trigger_ms = millis();
        }
    } else {
        consecutive_transport_recovery = 0;
    }
}

const uint32_t HW_WDT_SEC = 30;
uint32_t consecutivePublishFail = 0;
uint32_t consecutiveSlowPublishes = 0;

enum class PendingFrameState : uint8_t {
    EMPTY = 0,
    READY,
    PUBLISHING
};

struct PendingFrame {
    volatile PendingFrameState state = PendingFrameState::EMPTY;
    size_t jpegSize = 0;
    size_t base64Size = 0;
    uint32_t timestamp = 0;
    char *base64 = nullptr;
    const char* topic = nullptr;
};
PendingFrame pendingFrame;
static portMUX_TYPE pendingFrameMux = portMUX_INITIALIZER_UNLOCKED;

SemaphoreHandle_t cameraMutex = NULL;
SemaphoreHandle_t fbSemaphore = NULL;
TaskHandle_t publisherTaskHandle = NULL;
TaskHandle_t cameraTaskHandle = NULL;

void CameraCaptureTask(void* pvParameters);
void ImagePublisherTask(void* pvParameters);

unsigned long lastTransportRecoveryMillis = 0;
unsigned long lastWsConnectMillis = 0;
uint32_t wsOpenCount = 0;
uint32_t wsCloseCount = 0;
unsigned long lastPublishAttemptMillis = 0;
uint32_t publishOverIntervalCount = 0;
uint32_t telemetryPublishFailCount = 0;



OTAState currentOtaState = OTA_IDLE;
bool otaRunning = false;
volatile bool otaRequested = false;
unsigned long otaRequestTimestamp = 0;
String otaRequestManifestUrl = "";

String otaVersion = "";
String otaBoard = "";
String otaModel = "";
String otaMinVersion = "";
bool otaForce = false;
size_t otaSize = 0;
String otaSha256 = "";
String otaUrl = "";
bool otaRollbackAllowed = false;

String otaFailReason = "";
volatile bool otaCancelled = false;
size_t otaBytesDownloaded = 0;

unsigned long otaStartTime = 0;
unsigned long otaDownloadStartTime = 0;
unsigned long otaDownloadEndTime = 0;
unsigned long otaFlashStartTime = 0;
unsigned long otaFlashEndTime = 0;


const char* otaStateToString(OTAState state) {
    switch (state) {
        case OTA_IDLE: return "IDLE";
        case OTA_DOWNLOAD_MANIFEST: return "DOWNLOAD_MANIFEST";
        case OTA_VALIDATE: return "VALIDATE";
        case OTA_DOWNLOAD_FIRMWARE: return "DOWNLOAD_FIRMWARE";
        case OTA_VERIFY: return "VERIFY";
        case OTA_FLASH: return "FLASH";
        case OTA_SUCCESS: return "SUCCESS";
        case OTA_FAILED: return "FAILED";
        case OTA_CANCELLED: return "CANCELLED";
        default: return "UNKNOWN";
    }
}

const char base64_chars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

// ==========================================
// THREAD-SAFE PUBLISH DISPATCHER
// ==========================================
#define MAX_PUBLISH_REQUESTS 8
static PublishRequest publishQueue[MAX_PUBLISH_REQUESTS];
static volatile int publishQueueHead = 0;
static volatile int publishQueueTail = 0;
static portMUX_TYPE publishQueueMux = portMUX_INITIALIZER_UNLOCKED;

volatile const char* recoveryRequestReason = nullptr;
volatile bool mqttDisconnectRequested = false;
volatile bool mqttIsConnected = false;
volatile int mqttState = -1;

static char telemetry_publish_buffer[2048];
static char ota_publish_buffer[384];
static char config_publish_buffer[1024];

static portMUX_TYPE otaPublishMux = portMUX_INITIALIZER_UNLOCKED;
static portMUX_TYPE configPublishMux = portMUX_INITIALIZER_UNLOCKED;
static portMUX_TYPE telemetryPublishMux = portMUX_INITIALIZER_UNLOCKED;

bool enqueuePublish(const char* topic, const char* payload, bool retained) {
    if (!topic || !payload) return false;
    bool success = false;
    portENTER_CRITICAL(&publishQueueMux);
    int nextTail = (publishQueueTail + 1) % MAX_PUBLISH_REQUESTS;

    // Priority Admission Policy
    if (nextTail == publishQueueHead) {
        bool isTelemetry = (strstr(topic, "/telemetry") != nullptr);
        if (!isTelemetry) {
            int current = publishQueueHead;
            int foundIdx = -1;
            while (current != publishQueueTail) {
                if (strstr(publishQueue[current].topic, "/telemetry") != nullptr) {
                    foundIdx = current;
                    break;
                }
                current = (current + 1) % MAX_PUBLISH_REQUESTS;
            }
            if (foundIdx != -1) {
                int currIdx = foundIdx;
                while (currIdx != publishQueueHead) {
                    int prevIdx = (currIdx - 1 + MAX_PUBLISH_REQUESTS) % MAX_PUBLISH_REQUESTS;
                    publishQueue[currIdx] = publishQueue[prevIdx];
                    currIdx = prevIdx;
                }
                publishQueueHead = (publishQueueHead + 1) % MAX_PUBLISH_REQUESTS;
                publishQueueDropCount++;
                nextTail = (publishQueueTail + 1) % MAX_PUBLISH_REQUESTS;
            }
        }
    }

    if (nextTail != publishQueueHead) {
        strncpy(publishQueue[publishQueueTail].topic, topic, sizeof(publishQueue[publishQueueTail].topic) - 1);
        publishQueue[publishQueueTail].topic[sizeof(publishQueue[publishQueueTail].topic) - 1] = '\0';

        strncpy(publishQueue[publishQueueTail].payload, payload, sizeof(publishQueue[publishQueueTail].payload) - 1);
        publishQueue[publishQueueTail].payload[sizeof(publishQueue[publishQueueTail].payload) - 1] = '\0';

        publishQueue[publishQueueTail].retained = retained;
        publishQueueTail = nextTail;
        success = true;

        int depth = (publishQueueTail - publishQueueHead + MAX_PUBLISH_REQUESTS) % MAX_PUBLISH_REQUESTS;
        if (depth > (int)publishQueuePeak) {
            publishQueuePeak = depth;
        }
    } else {
        publishQueueOverflowCount++;
    }
    portEXIT_CRITICAL(&publishQueueMux);
    if (success && publisherTaskHandle != NULL) {
        xTaskNotifyGive(publisherTaskHandle);
    }
    return success;
}

bool dequeuePublish(PublishRequest& req) {
    bool success = false;
    portENTER_CRITICAL(&publishQueueMux);
    if (publishQueueHead != publishQueueTail) {
        req = publishQueue[publishQueueHead];
        publishQueueHead = (publishQueueHead + 1) % MAX_PUBLISH_REQUESTS;
        success = true;
    }
    portEXIT_CRITICAL(&publishQueueMux);
    return success;
}

void requestRecovery(const char* reason) {
    recoveryRequestReason = reason;
}

bool encode_base64(const uint8_t* input, size_t input_len, char* output, size_t output_max_len, size_t& output_len) {
    size_t required_len = ((input_len + 2) / 3) * 4;
    if (required_len + 1 > output_max_len) {
        return false;
    }
    
    size_t i = 0;
    size_t j = 0;
    size_t yield_counter = 0;
    while (i < input_len) {
        if (++yield_counter % 1024 == 0) {
            yield();
        }
        size_t remaining = input_len - i;
        if (remaining >= 3) {
            uint32_t triple = (input[i] << 16) | (input[i+1] << 8) | input[i+2];
            output[j++] = base64_chars[(triple >> 18) & 0x3F];
            output[j++] = base64_chars[(triple >> 12) & 0x3F];
            output[j++] = base64_chars[(triple >> 6) & 0x3F];
            output[j++] = base64_chars[triple & 0x3F];
            i += 3;
        } else if (remaining == 2) {
            uint32_t triple = (input[i] << 16) | (input[i+1] << 8);
            output[j++] = base64_chars[(triple >> 18) & 0x3F];
            output[j++] = base64_chars[(triple >> 12) & 0x3F];
            output[j++] = base64_chars[(triple >> 6) & 0x3F];
            output[j++] = '=';
            i += 2;
        } else { // remaining == 1
            uint32_t triple = (input[i] << 16);
            output[j++] = base64_chars[(triple >> 18) & 0x3F];
            output[j++] = base64_chars[(triple >> 12) & 0x3F];
            output[j++] = '=';
            output[j++] = '=';
            i += 1;
        }
    }
    output[j] = '\0';
    output_len = j;
    return true;
}

bool canPerformReboot() {
    bool camera_failed_10 = (camera_consecutive_fails >= 10);
    bool mqtt_unavailable_30m = (mqtt_unavailable_start > 0 && (millis() - mqtt_unavailable_start >= 1800000UL));
    bool transport_recovery_exhausted = (transportRecoveryCount >= MAX_TRANSPORT_RECOVERY);
    return camera_failed_10 && mqtt_unavailable_30m && transport_recovery_exhausted;
}

bool initCamera(bool rebootOnFail = true);

void handleSoftRecovery(const char* reason) {
    if (otaRunning) return; // Edit 1: Suspend soft recovery during OTA
    lastRecoveryReason = String(reason);
    consecutive_failures++;
    last_failure_free_time = millis();
    if (consecutive_failures == 1) {
        recovery_start_time = millis();
    }
    
    if (suspend_capture_until > millis()) {
        recovery_state = "SUSPENDED";
    } else if (consecutive_failures > 0) {
        recovery_state = "DEGRADED";
    } else {
        recovery_state = "NORMAL";
    }
    
    switch (consecutive_failures) {
        case 1:
            break;
        case 2:
            mqttDisconnectRequested = true;
            break;
        case 3:
            if (xSemaphoreTake(fbSemaphore, portMAX_DELAY) == pdTRUE) {
                if (xSemaphoreTake(cameraMutex, portMAX_DELAY) == pdTRUE) {
                    esp_camera_deinit();
                    yield(); vTaskDelay(1000 / portTICK_PERIOD_MS);
                    initCamera(false);
                    xSemaphoreGive(cameraMutex);
                }
                xSemaphoreGive(fbSemaphore);
            }
            break;
        case 4:
            WiFi.disconnect(false);
            yield(); vTaskDelay(1000 / portTICK_PERIOD_MS);
            WiFi.reconnect();
            break;
        case 5:
            suspend_capture_until = millis() + 300000UL;
            recovery_state = "SUSPENDED";
            break;
        case 6:
            if (canPerformReboot()) {
                yield(); vTaskDelay(1000 / portTICK_PERIOD_MS);
                ESP.restart();
            } else {
                consecutive_failures = 5;
                suspend_capture_until = millis() + 300000UL;
                recovery_state = "SUSPENDED";
            }
            break;
        default:
            if (consecutive_failures > 6) {
                if (canPerformReboot()) {
                    yield(); vTaskDelay(1000 / portTICK_PERIOD_MS);
                    ESP.restart();
                } else {
                    consecutive_failures = 5;
                    suspend_capture_until = millis() + 300000UL;
                    recovery_state = "SUSPENDED";
                }
            }
            break;
    }
}

void transitionOtaState(OTAState newState) {
    currentOtaState = newState;
    bool wasOtaRunning = otaRunning;
    otaRunning = (currentOtaState != OTA_IDLE);
    if (otaRunning && !wasOtaRunning) {
        portENTER_CRITICAL(&pendingFrameMux);
        pendingFrame.state = PendingFrameState::EMPTY;
        portEXIT_CRITICAL(&pendingFrameMux);
    }
}

void publishOtaStatus(const char* status, int progress = -1, const char* messageOrReason = "", const char* version = "") {
    portENTER_CRITICAL(&otaPublishMux);
#if ARDUINOJSON_VERSION_MAJOR >= 7
    JsonDocument doc;
#else
    StaticJsonDocument<256> doc;
#endif
    
    doc["status"] = status;
    if (progress >= 0) {
        doc["progress"] = progress;
    }
    if (messageOrReason && strlen(messageOrReason) > 0) {
        if (strcmp(status, "OTA_FAILED") == 0) {
            doc["reason"] = messageOrReason;
        } else {
            doc["message"] = messageOrReason;
        }
    }
    if (version && strlen(version) > 0) {
        doc["version"] = version;
    }
    
    serializeJson(doc, ota_publish_buffer, sizeof(ota_publish_buffer));
    enqueuePublish(topic_ota_status.c_str(), ota_publish_buffer, true);
    portEXIT_CRITICAL(&otaPublishMux);
}

void logOtaFailure() {}

bool isVersionGreater(const String& newVer, const String& currVer) {
    int newMajor = 0, newMinor = 0, newPatch = 0;
    int currMajor = 0, currMinor = 0, currPatch = 0;
    sscanf(newVer.c_str(), "%d.%d.%d", &newMajor, &newMinor, &newPatch);
    sscanf(currVer.c_str(), "%d.%d.%d", &currMajor, &currMinor, &currPatch);
    if (newMajor != currMajor) return newMajor > currMajor;
    if (newMinor != currMinor) return newMinor > currMinor;
    return newPatch > currPatch;
}

void resetOtaState() {
    // Edit 5: Clean up all OTA HTTP/TLS and hash resources
    otaHttp.end();
    otaClient.stop();
    if (otaHasher) {
        delete otaHasher;
        otaHasher = nullptr;
    }
    currentOtaState = OTA_IDLE;
    otaRunning = false;
    otaRequested = false;
    otaRequestTimestamp = 0;
    otaRequestManifestUrl = "";
    otaVersion = "";
    otaBoard = "";
    otaModel = "";
    otaMinVersion = "";
    otaForce = false;
    otaSize = 0;
    otaSha256 = "";
    otaUrl = "";
    otaRollbackAllowed = false;
    otaFailReason = "";
    otaCancelled = false;
    otaBytesDownloaded = 0;
    otaStartTime = 0;
    otaDownloadStartTime = 0;
    otaDownloadEndTime = 0;
    otaFlashStartTime = 0;
    otaFlashEndTime = 0;
}

void handleOtaCancel(HTTPClient& http) {
    Update.abort();
    http.end();
    transitionOtaState(OTA_CANCELLED);
    publishOtaStatus("OTA_CANCELLED");
    resetOtaState();
}

void cleanupAndFail(HTTPClient& http, const String& reason) {
    otaFailReason = reason;
    Update.abort();
    http.end();
    transitionOtaState(OTA_FAILED);
    publishOtaStatus("OTA_FAILED", -1, otaFailReason.c_str());
    resetOtaState();
}

bool parseManifest(const String& manifestContent) {
#if ARDUINOJSON_VERSION_MAJOR >= 7
    JsonDocument doc;
#else
    StaticJsonDocument<1024> doc;
#endif
    
    DeserializationError error = deserializeJson(doc, manifestContent);
    if (error) {
        otaFailReason = "Manifest JSON parsing failed";
        return false;
    }
    
    otaVersion = doc["version"].as<String>();
    otaBoard = doc.containsKey("board") ? doc["board"].as<String>() : "";
    otaModel = doc.containsKey("model") ? doc["model"].as<String>() : "";
    otaMinVersion = doc.containsKey("min_version") ? doc["min_version"].as<String>() : "";
    otaForce = doc.containsKey("force") ? doc["force"].as<bool>() : false;
    otaSize = doc["size"].as<size_t>();
    otaSha256 = doc.containsKey("sha256") ? doc["sha256"].as<String>() : "";
    otaUrl = doc["url"].as<String>();
    otaRollbackAllowed = doc.containsKey("rollback_allowed") ? doc["rollback_allowed"].as<bool>() : false;
    
    return true;
}

bool validateManifest() {
    if (otaUrl.length() == 0) {
        otaFailReason = "URL missing";
        return false;
    }
    if (otaBoard != FW_BOARD) {
        otaFailReason = "board mismatch";
        return false;
    }
    if (otaModel != FW_MODEL) {
        otaFailReason = "model mismatch";
        return false;
    }
    if (otaSize == 0) {
        otaFailReason = "Firmware size == 0";
        return false;
    }
    if (otaSize > 2097152) {
        otaFailReason = "Firmware exceeds maximum supported size";
        return false;
    }
    size_t freeSpace = ESP.getFreeSketchSpace();
    if (otaSize > freeSpace) {
        otaFailReason = "Firmware size > free OTA space";
        return false;
    }
    if (!isVersionGreater(otaVersion, FW_VERSION) && !otaForce) {
        otaFailReason = "Version <= current version";
        return false;
    }
    if (otaMinVersion.length() > 0 && isVersionGreater(otaMinVersion, FW_VERSION)) {
        otaFailReason = "Version < min_version";
        return false;
    }
    return true;
}

void startOta() {
    otaRequested = false;
    otaRunning = true;
    otaStartTime = millis();
    otaBytesDownloaded = 0;
    otaRetryCount = 0;
    otaBackoffMs = 1000;
    otaInWait = false;
    otaManifestContent = "";
    transitionOtaState(OTA_DOWNLOAD_MANIFEST);
    publishOtaStatus("OTA_START");
}

void tickOtaStateMachine() {
    if (!otaRunning) return;

    if (otaCancelled && currentOtaState != OTA_CANCELLED) {
        Update.abort();
        otaHttp.end();
        transitionOtaState(OTA_CANCELLED);
        publishOtaStatus("OTA_CANCELLED");
        resetOtaState();
        return;
    }

    switch (currentOtaState) {
        case OTA_DOWNLOAD_MANIFEST: {
            if (otaInWait) {
                if (millis() - otaWaitStart >= (unsigned long)otaBackoffMs) {
                    otaInWait = false;
                    otaBackoffMs *= 2;
                } else {
                    yield();
                    vTaskDelay(1);
                    return;
                }
            }

            otaClient.setInsecure();
            otaHttp.setTimeout(30000); // Reverted to 30s
            otaHttp.setFollowRedirects(HTTPC_FORCE_FOLLOW_REDIRECTS);

            if (otaHttp.begin(otaClient, otaRequestManifestUrl)) {
                int httpCode = otaHttp.GET();
                if (httpCode == HTTP_CODE_OK) {
                    otaManifestContent = otaHttp.getString();
                    otaHttp.end();
                    transitionOtaState(OTA_VALIDATE);
                } else {
                    otaHttp.end();
                    otaRetryCount++;
                    if (otaRetryCount >= 3) {
                        otaFailReason = "Manifest download failed after 3 retries";
                        transitionOtaState(OTA_FAILED);
                    } else {
                        otaWaitStart = millis();
                        otaInWait = true;
                    }
                }
            } else {
                otaRetryCount++;
                if (otaRetryCount >= 3) {
                    otaFailReason = "Manifest http.begin failed after 3 retries";
                    transitionOtaState(OTA_FAILED);
                } else {
                    otaWaitStart = millis();
                    otaInWait = true;
                }
            }
            break;
        }

        case OTA_VALIDATE: {
            if (!parseManifest(otaManifestContent) || !validateManifest()) {
                transitionOtaState(OTA_FAILED);
            } else {
                transitionOtaState(OTA_DOWNLOAD_FIRMWARE);
                otaRetryCount = 0;
                otaBackoffMs = 1000;
                otaInWait = false;
            }
            break;
        }

        case OTA_DOWNLOAD_FIRMWARE: {
            if (otaInWait) {
                if (millis() - otaWaitStart >= (unsigned long)otaBackoffMs) {
                    otaInWait = false;
                    otaBackoffMs *= 2;
                } else {
                    yield();
                    vTaskDelay(1);
                    return;
                }
            }

            otaClient.setInsecure();
            otaHttp.setTimeout(120000);
            otaHttp.setFollowRedirects(HTTPC_FORCE_FOLLOW_REDIRECTS);
            const char* headerKeys[] = {"Content-Type"};
            otaHttp.collectHeaders(headerKeys, 1);

            if (otaHttp.begin(otaClient, otaUrl)) {
                int httpCode = otaHttp.GET();
                if (httpCode == HTTP_CODE_OK) {
                    int totalLength = otaHttp.getSize();
                    if (totalLength > 0 && (size_t)totalLength == otaSize) {
                        otaFlashStartTime = millis();
                        if (!Update.begin(otaSize, U_FLASH)) {
                            otaFailReason = "Update.begin failed: " + String(Update.errorString());
                            otaHttp.end();
                            transitionOtaState(OTA_FAILED);
                        } else {
                            otaWritten = 0;
                            otaLastProgress = 0;
                            otaLastHeartbeatTime = millis();
                            otaLastDataMillis = millis();
                            if (otaHasher) delete otaHasher;
                            otaHasher = new SHA256Hasher();
                            otaDownloadStartTime = millis();
                            transitionOtaState(OTA_FLASH);
                            publishOtaStatus("OTA_FLASH");
                            publishOtaStatus("OTA_PROGRESS", 0);
                        }
                    } else {
                        otaFailReason = "Firmware size mismatch";
                        otaHttp.end();
                        transitionOtaState(OTA_FAILED);
                    }
                } else {
                    otaFailReason = "HTTP " + String(httpCode);
                    otaHttp.end();
                    otaRetryCount++;
                    if (otaRetryCount >= 3) {
                        transitionOtaState(OTA_FAILED);
                    } else {
                        otaWaitStart = millis();
                        otaInWait = true;
                    }
                }
            } else {
                otaFailReason = "Firmware http.begin failed";
                otaRetryCount++;
                if (otaRetryCount >= 3) {
                    transitionOtaState(OTA_FAILED);
                } else {
                    otaWaitStart = millis();
                    otaInWait = true;
                }
            }
            break;
        }

        case OTA_FLASH: {
            WiFiClient* stream = otaHttp.getStreamPtr();
            if (!stream) {
                otaFailReason = "Get stream failed";
                Update.abort();
                otaHttp.end();
                transitionOtaState(OTA_FAILED);
                return;
            }

            bool is_connected = stream->connected();
            size_t available = stream->available();

            if (millis() - otaFlashStartTime > 300000) {
                otaFailReason = "Flash timeout (300s)";
                Update.abort();
                otaHttp.end();
                transitionOtaState(OTA_FAILED);
                return;
            }

            if (!is_connected && available == 0) {
                otaFailReason = "Connection lost (stream disconnected)";
                Update.abort();
                otaHttp.end();
                transitionOtaState(OTA_FAILED);
                return;
            }

            // Edit 4: Flat 45s download stall check (no connected bonus)
            if (millis() - otaLastDataMillis > 45000) {
                otaFailReason = "Download stalled (no data for 45s)";
                Update.abort();
                otaHttp.end();
                transitionOtaState(OTA_FAILED);
                return;
            }

            // Edit 2: Change vTaskDelay from 1 to 5 when no data is available
            if (available == 0) {
                yield();
                vTaskDelay(pdMS_TO_TICKS(5));
                return;
            }

            // Draining Loop: drain up to OTA_MAX_BYTES_PER_TICK of TLS buffer in one tick
            constexpr size_t OTA_CHUNK_SIZE = 2048;
            constexpr size_t OTA_MAX_BYTES_PER_TICK = 8192;
            
            bool writeError = false;
            size_t bytesProcessed = 0;
            
            while (available > 0 && otaWritten < otaSize && bytesProcessed < OTA_MAX_BYTES_PER_TICK) {
                static uint8_t buff[OTA_CHUNK_SIZE];
                // Edit: Safe remaining check to prevent reading beyond expected otaSize
                size_t remaining = (otaSize > otaWritten) ? (otaSize - otaWritten) : 0;
                size_t toRead = std::min((size_t)available, sizeof(buff));
                toRead = std::min(toRead, remaining);
                
                int readBytes = stream->readBytes((char*)buff, toRead);
                
                // Edit 3: Explicitly handle readBytes <= 0
                if (readBytes <= 0) {
                    break;
                }
                
                otaLastDataMillis = millis();
                
                if (otaSha256.length() > 0 && otaHasher) {
                    otaHasher->update(buff, readBytes);
                }
                
                size_t w = Update.write(buff, readBytes);
                
                if (w != (size_t)readBytes) {
                    otaFailReason = "Flash write failed: size mismatch (" + String(w) + "/" + String(readBytes) + ")";
                    writeError = true;
                    break;
                }
                
                otaWritten += readBytes;
                otaBytesDownloaded = otaWritten;
                bytesProcessed += readBytes;

                int progress = (otaSize > 0) ? (otaWritten * 100) / otaSize : 0;
                int progressTen = (progress / 10) * 10;
                if (progressTen > otaLastProgress) {
                    otaLastProgress = progressTen;
                    publishOtaStatus("OTA_PROGRESS", otaLastProgress);
                }
                
                available = stream->available();
            }

            if (writeError) {
                Update.abort();
                otaHttp.end();
                transitionOtaState(OTA_FAILED);
                return;
            }

            if (available == 0) {
                yield();
                vTaskDelay(pdMS_TO_TICKS(5));
            }

            if (millis() - otaLastHeartbeatTime >= 5000) {
                otaLastHeartbeatTime = millis();
                int progressVal = (otaSize > 0) ? (otaWritten * 100) / otaSize : 0;
                publishOtaStatus("OTA_RUNNING", progressVal);
            }

            if (otaWritten >= otaSize) {
                otaDownloadEndTime = millis();
                otaHttp.end();
                transitionOtaState(OTA_VERIFY);
            }
            break;
        }

        case OTA_VERIFY: {
            publishOtaStatus("OTA_VERIFY");
            if (otaSha256.length() > 0 && otaHasher) {
                String calculatedSha = otaHasher->finish();
                if (calculatedSha != otaSha256) {
                    otaFailReason = "SHA256 mismatch";
                    Update.abort();
                    transitionOtaState(OTA_FAILED);
                    return;
                }
            }
            otaFlashEndTime = millis();
            if (!Update.end(true)) {
                otaFailReason = "Update.end failed: " + String(Update.errorString());
                transitionOtaState(OTA_FAILED);
            } else {
                transitionOtaState(OTA_SUCCESS);
            }
            break;
        }

        case OTA_SUCCESS: {
            publishOtaStatus("OTA_SUCCESS", 100, "Firmware updated.", otaVersion.c_str());
            if (otaHasher) {
                delete otaHasher;
                otaHasher = nullptr;
            }
            resetOtaState();
            vTaskDelay(pdMS_TO_TICKS(2000));
            ESP.restart();
            break;
        }

        case OTA_FAILED: {
            publishOtaStatus("OTA_FAILED", -1, otaFailReason.c_str());
            if (otaHasher) {
                delete otaHasher;
                otaHasher = nullptr;
            }
            resetOtaState();
            break;
        }

        case OTA_CANCELLED: {
            publishOtaStatus("OTA_CANCELLED");
            if (otaHasher) {
                delete otaHasher;
                otaHasher = nullptr;
            }
            resetOtaState();
            break;
        }

        default:
            break;
    }
}

framesize_t stringToFrameSize(const String& fs) {
    if (fs == "QQVGA") return FRAMESIZE_QQVGA;
    if (fs == "QCIF") return FRAMESIZE_QCIF;
    if (fs == "HQVGA") return FRAMESIZE_HQVGA;
    if (fs == "240X240") return FRAMESIZE_240X240;
    if (fs == "QVGA") return FRAMESIZE_QVGA;
    if (fs == "CIF") return FRAMESIZE_CIF;
    if (fs == "HVGA") return FRAMESIZE_HVGA;
    if (fs == "VGA") return FRAMESIZE_VGA;
    if (fs == "SVGA") return FRAMESIZE_SVGA;
    if (fs == "XGA") return FRAMESIZE_XGA;
    if (fs == "HD") return FRAMESIZE_HD;
    if (fs == "SXGA") return FRAMESIZE_SXGA;
    if (fs == "UXGA") return FRAMESIZE_UXGA;
    return (framesize_t)-1;
}

String frameSizeToString(framesize_t fs) {
    switch (fs) {
        case FRAMESIZE_QQVGA: return "QQVGA";
        case FRAMESIZE_QCIF: return "QCIF";
        case FRAMESIZE_HQVGA: return "HQVGA";
        case FRAMESIZE_240X240: return "240X240";
        case FRAMESIZE_QVGA: return "QVGA";
        case FRAMESIZE_CIF: return "CIF";
        case FRAMESIZE_HVGA: return "HVGA";
        case FRAMESIZE_VGA: return "VGA";
        case FRAMESIZE_SVGA: return "SVGA";
        case FRAMESIZE_XGA: return "XGA";
        case FRAMESIZE_HD: return "HD";
        case FRAMESIZE_SXGA: return "SXGA";
        case FRAMESIZE_UXGA: return "UXGA";
        default: return "UNKNOWN";
    }
}

void publishConfigStatus(const char* status, const char* messageOrReason, bool success) {
    portENTER_CRITICAL(&configPublishMux);
#if ARDUINOJSON_VERSION_MAJOR >= 7
    JsonDocument doc;
#else
    StaticJsonDocument<512> doc;
#endif
    
    doc["status"] = status;
    if (success) {
        doc["message"] = messageOrReason;
        doc["applied"]["jpeg_quality"] = current_jpeg_quality;
        doc["applied"]["frame_size"] = frameSizeToString(current_frame_size);
        doc["applied"]["capture_interval_ms"] = capture_interval_ms;
        doc["applied"]["telemetry_interval_ms"] = telemetry_interval_ms;
        doc["applied"]["image_enabled"] = image_enabled;
        doc["applied"]["telemetry_enabled"] = telemetry_enabled;
        doc["applied"]["ota_enabled"] = ota_enabled;
        doc["applied"]["mqtt_buffer"] = mqttBufferSize;
    } else {
        doc["reason"] = messageOrReason;
    }
    
    serializeJson(doc, config_publish_buffer, sizeof(config_publish_buffer));
    enqueuePublish(topic_config_status.c_str(), config_publish_buffer, true);
    portEXIT_CRITICAL(&configPublishMux);
}

void handleConfigPayload(byte* payload, unsigned int length) {
#if ARDUINOJSON_VERSION_MAJOR >= 7
    JsonDocument doc;
#else
    DynamicJsonDocument doc(1024);
#endif
    
    DeserializationError error = deserializeJson(doc, (const char*)payload, length);
    if (error) {
        publishConfigStatus("CONFIG_FAILED", "Malformed JSON", false);
        return;
    }
    
    JsonVariant cfg = doc;
    if (doc.containsKey("config")) {
        cfg = doc["config"];
    }

    // Validate parameters
    int new_jpeg_quality = current_jpeg_quality;
    if (cfg.containsKey("jpeg_quality")) {
        if (!cfg["jpeg_quality"].is<int>()) {
            publishConfigStatus("CONFIG_FAILED", "jpeg_quality must be an integer", false);
            return;
        }
        int val = cfg["jpeg_quality"].as<int>();
        if (val < 10 || val > 63) {
            publishConfigStatus("CONFIG_FAILED", "jpeg_quality must be between 10 and 63", false);
            return;
        }
        new_jpeg_quality = val;
    }

    framesize_t new_frame_size = current_frame_size;
    if (cfg.containsKey("frame_size")) {
        if (!cfg["frame_size"].is<const char*>()) {
            publishConfigStatus("CONFIG_FAILED", "frame_size must be a string", false);
            return;
        }
        String val = cfg["frame_size"].as<String>();
        framesize_t fs = stringToFrameSize(val);
        if (fs == (framesize_t)-1) {
            publishConfigStatus("CONFIG_FAILED", "Invalid frame_size", false);
            return;
        }
        new_frame_size = fs;
    }

    unsigned long new_capture_interval_ms = capture_interval_ms;
    if (cfg.containsKey("capture_interval_ms")) {
        if (!cfg["capture_interval_ms"].is<unsigned long>() && !cfg["capture_interval_ms"].is<int>()) {
            publishConfigStatus("CONFIG_FAILED", "capture_interval_ms must be an integer", false);
            return;
        }
        long val = cfg["capture_interval_ms"].as<long>();
        if (val < 500) {
            publishConfigStatus("CONFIG_FAILED", "capture_interval_ms must be at least 500", false);
            return;
        }
        new_capture_interval_ms = (unsigned long)val;
    }

    unsigned long new_telemetry_interval_ms = telemetry_interval_ms;
    if (cfg.containsKey("telemetry_interval_ms")) {
        if (!cfg["telemetry_interval_ms"].is<unsigned long>() && !cfg["telemetry_interval_ms"].is<int>()) {
            publishConfigStatus("CONFIG_FAILED", "telemetry_interval_ms must be an integer", false);
            return;
        }
        long val = cfg["telemetry_interval_ms"].as<long>();
        if (val < 1000) {
            publishConfigStatus("CONFIG_FAILED", "telemetry_interval_ms must be at least 1000", false);
            return;
        }
        new_telemetry_interval_ms = (unsigned long)val;
    }

    bool new_image_enabled = image_enabled;
    if (cfg.containsKey("image_enabled")) {
        if (!cfg["image_enabled"].is<bool>()) {
            publishConfigStatus("CONFIG_FAILED", "image_enabled must be a boolean", false);
            return;
        }
        new_image_enabled = cfg["image_enabled"].as<bool>();
    }

    bool new_telemetry_enabled = telemetry_enabled;
    if (cfg.containsKey("telemetry_enabled")) {
        if (!cfg["telemetry_enabled"].is<bool>()) {
            publishConfigStatus("CONFIG_FAILED", "telemetry_enabled must be a boolean", false);
            return;
        }
        new_telemetry_enabled = cfg["telemetry_enabled"].as<bool>();
    }

    bool new_ota_enabled = ota_enabled;
    if (cfg.containsKey("ota_enabled")) {
        if (!cfg["ota_enabled"].is<bool>()) {
            publishConfigStatus("CONFIG_FAILED", "ota_enabled must be a boolean", false);
            return;
        }
        new_ota_enabled = cfg["ota_enabled"].as<bool>();
    }

    int new_mqtt_buffer = mqttBufferSize;
    if (cfg.containsKey("mqtt_buffer")) {
        if (!cfg["mqtt_buffer"].is<int>()) {
            publishConfigStatus("CONFIG_FAILED", "mqtt_buffer must be an integer", false);
            return;
        }
        int val = cfg["mqtt_buffer"].as<int>();
        if (val < 1024 || val > 131072) {
            publishConfigStatus("CONFIG_FAILED", "mqtt_buffer must be between 1024 and 131072", false);
            return;
        }
        new_mqtt_buffer = val;
    }

    // Apply camera configuration (safe camera re-init if changed)
    bool cameraChanged = (new_frame_size != current_frame_size || new_jpeg_quality != current_jpeg_quality);
    if (cameraChanged) {
        framesize_t old_frame_size = current_frame_size;
        int old_jpeg_quality = current_jpeg_quality;
        
        current_frame_size = new_frame_size;
        current_jpeg_quality = new_jpeg_quality;
        
        if (xSemaphoreTake(fbSemaphore, portMAX_DELAY) == pdTRUE) {
            if (xSemaphoreTake(cameraMutex, portMAX_DELAY) == pdTRUE) {
                esp_camera_deinit();
                vTaskDelay(pdMS_TO_TICKS(1000));
                if (!initCamera(false)) {
                    // Rollback
                    current_frame_size = old_frame_size;
                    current_jpeg_quality = old_jpeg_quality;
                    esp_camera_deinit();
                    vTaskDelay(pdMS_TO_TICKS(1000));
                    initCamera(false);
                    xSemaphoreGive(cameraMutex);
                    xSemaphoreGive(fbSemaphore);
                    publishConfigStatus("CONFIG_FAILED", "Camera re-init failed", false);
                    return;
                }
                xSemaphoreGive(cameraMutex);
            }
            xSemaphoreGive(fbSemaphore);
        }
    }

    // Apply MQTT buffer configuration
    bool mqttBufferChanged = (new_mqtt_buffer != mqttBufferSize);
    if (mqttBufferChanged) {
        if (!mqttClient.setBufferSize(new_mqtt_buffer)) {
            publishConfigStatus("CONFIG_FAILED", "Failed to set MQTT buffer size", false);
            return;
        }
        mqttBufferSize = mqttClient.getBufferSize();
    }

    // Apply other parameters
    capture_interval_ms = new_capture_interval_ms;
    telemetry_interval_ms = new_telemetry_interval_ms;
    image_enabled = new_image_enabled;
    telemetry_enabled = new_telemetry_enabled;
    ota_enabled = new_ota_enabled;

    publishConfigStatus("CONFIG_SUCCESS", "Applied", true);
}

void mqttCallback(char* topic, byte* payload, unsigned int length) {
    if (String(topic) == topic_ota) {
#if ARDUINOJSON_VERSION_MAJOR >= 7
        JsonDocument doc;
#else
        StaticJsonDocument<512> doc;
#endif
        
        DeserializationError error = deserializeJson(doc, (const char*)payload, length);
        if (error) {
            return;
        }
        
        if (!doc.containsKey("action")) {
            return;
        }
        
        String action = doc["action"].as<String>();

        if (action == "ota") {
            if (!ota_enabled) {
                publishOtaStatus("OTA_FAILED", -1, "OTA disabled");
                return;
            }
            if (!doc.containsKey("manifest")) {
                publishOtaStatus("OTA_FAILED", -1, "URL missing");
                return;
            }
            if (otaRunning || otaRequested) {
                publishOtaStatus("OTA_FAILED", -1, "OTA already running");
                return;
            }
            otaRequestManifestUrl = doc["manifest"].as<String>();
            otaRequested = true;
            otaRequestTimestamp = millis();
        } else if (action == "cancel") {
            if (otaRunning) {
                otaCancelled = true;
            } else {
                publishOtaStatus("OTA_FAILED", -1, "No OTA running");
            }
        } else if (action == "check") {
#if ARDUINOJSON_VERSION_MAJOR >= 7
            JsonDocument checkDoc;
#else
            StaticJsonDocument<128> checkDoc;
#endif
            checkDoc["status"] = "OTA_CHECK";
            checkDoc["version"] = FW_VERSION;
            
            portENTER_CRITICAL(&otaPublishMux);
            serializeJson(checkDoc, ota_publish_buffer, sizeof(ota_publish_buffer));
            enqueuePublish(topic_ota_status.c_str(), ota_publish_buffer, true);
            portEXIT_CRITICAL(&otaPublishMux);
        }
    } else if (String(topic) == topic_config) {
        String payloadStr = "";
        for (unsigned int i = 0; i < length; i++) {
            payloadStr += (char)payload[i];
        }
        if (payloadStr == "restart") {
            remoteRestartRequested = true;
            return;
        } else if (payloadStr == "camera_reinit") {
            remoteCameraReinitRequested = true;
            return;
        }
        handleConfigPayload(payload, length);
    }
}

String getResetReason() {
    esp_reset_reason_t reason = esp_reset_reason();
    switch (reason) {
        case ESP_RST_POWERON: return "POWERON";
        case ESP_RST_BROWNOUT: return "BROWNOUT";
        case ESP_RST_TASK_WDT: return "TASK_WDT";
        case ESP_RST_INT_WDT: return "INT_WDT";
        case ESP_RST_SW: return "SW_RESET";
        default: return "OTHER";
    }
}

void sendTelemetryInternal();

void sendTelemetry() {
    if (!telemetry_enabled) return;
    if (otaRunning) return;
    sendTelemetryInternal();
}

void sendTelemetryInternal() {
    if (!telemetry_enabled) return;
    if (otaRunning) return;

#if ARDUINOJSON_VERSION_MAJOR >= 7
    JsonDocument doc;
#else
    StaticJsonDocument<3072> doc;
#endif
    doc["device_id"] = device_id;
    doc["fw_version"] = FW_VERSION;
    doc["fw_board"] = FW_BOARD;
    doc["fw_model"] = FW_MODEL;
    doc["fw_build"] = FW_BUILD;
    doc["ota_supported"] = true;
    doc["ota_running"] = otaRunning;
    doc["ssid"] = WiFi.SSID();
    doc["bssid"] = WiFi.BSSIDstr();
    doc["channel"] = WiFi.channel();
    doc["ip_address"] = WiFi.localIP().toString();
    doc["mac_address"] = WiFi.macAddress();
    doc["hostname"] = DEVICE_HOSTNAME;
    doc["sketch_size"] = ESP.getSketchSize();
    doc["free_sketch_space"] = ESP.getFreeSketchSpace();
    doc["uptime_sec"] = millis() / 1000;
    doc["free_heap"] = ESP.getFreeHeap();
    doc["min_free_heap"] = ESP.getMinFreeHeap();
    doc["fb_null"] = fbNullCount;
    doc["rssi"] = WiFi.RSSI();
    doc["wifi_status"] = WiFi.status();

    doc["mqtt_connected"] = mqttIsConnected;
    doc["ws_connected"] = realWsConnected;
    doc["ws_uptime_sec"] = realWsConnected ? (millis() - lastWsConnectMillis) / 1000 : 0;
    doc["ws_open_count"] = wsOpenCount;
    doc["ws_close_count"] = wsCloseCount;
    doc["mqtt_state"] = mqttState;
    doc["mqtt_uptime_sec"] = mqttIsConnected ? (millis() - lastMqttConnectMillis) / 1000 : 0;
    doc["transport_state"] = transport_state;
    doc["last_publish_success_age_ms"] = (lastPublishSuccessMillis == 0) ? -1 : (int32_t)(millis() - lastPublishSuccessMillis);
    doc["consecutive_transport_recovery"] = consecutive_transport_recovery;
    doc["last_publish_result"] = last_publish_result;
    doc["last_connect_result"] = last_connect_result;
    doc["last_failed_publish_mqtt_state"] = lastFailedPublishMqttState;
    doc["last_failed_publish_wifi_status"] = (int)lastFailedPublishWifiStatus;
    doc["loop_counter"] = loopCounter;

    doc["capture_ok"] = captureOkCount;
    doc["capture_fail"] = captureFailCount;

    doc["publish_ok"] = publishOkCount;
    doc["publish_fail"] = publishFailCount;
    doc["telemetry_publish_fail"] = telemetryPublishFailCount;

    doc["wifi_drop"] = wifiDropCount;
    doc["mqtt_reconnect"] = mqttReconnectCount;
    doc["recovery_reason"] = lastRecoveryReason;
    doc["disconnect_reason"] = lastDisconnectReason;
    doc["last_image_sec"] = (lastCaptureSuccessMillis == 0) ? -1 : (int32_t)((millis() - lastCaptureSuccessMillis) / 1000);

    doc["reset_reason"] = getResetReason();

    if (suspend_capture_until > millis()) {
        recovery_state = "SUSPENDED";
    } else if (consecutive_failures > 0) {
        recovery_state = "DEGRADED";
    } else {
        recovery_state = "NORMAL";
    }

    doc["consecutive_failures"] = consecutive_failures;
    doc["recovery_level"] = consecutive_failures;
    doc["recovery_state"] = recovery_state;

    bool sendDiagnostics = (consecutive_failures > 0 || 
                            lastRecoveryReason != "NONE" || 
                            strcmp(lastDisconnectReason, "NONE") != 0 ||
                            transportRecoveryCount > 0 || 
                            wifiDropCount > 0 || 
                            publishQueueOverflowCount > 0 || 
                            publishQueueDropCount > 0);

    if (sendDiagnostics) {
        if (psramFound()) {
            doc["free_psram"] = ESP.getFreePsram();
        }
        uint32_t largestBlock = heap_caps_get_largest_free_block(MALLOC_CAP_8BIT);
        doc["largest_free_block"] = largestBlock;

        uint32_t freeHeap = ESP.getFreeHeap();
        uint32_t fragmentationPct = 0;
        if (freeHeap > 0 && largestBlock <= freeHeap) {
            fragmentationPct = 100 - ((largestBlock * 100) / freeHeap);
        }
        doc["fragmentation_pct"] = fragmentationPct;

        doc["publish_ms"] = publishMs;
        doc["max_publish_ms"] = maxPublishMs;
        doc["frame_size"] = frameSize;
        doc["base64_size"] = base64Size;
        doc["max_base64_size"] = maxBase64Size;
        doc["mqtt_buffer"] = mqttBufferSize;
        doc["max_frame_size"] = maxFrameSize;

        doc["publish_stall"] = publishStallCount;
        doc["publish_over_interval"] = publishOverIntervalCount;
        doc["transport_recovery"] = transportRecoveryCount;
        if (lastPublishAttemptMillis == 0) {
            doc["last_publish_attempt_sec"] = -1;
        } else {
            doc["last_publish_attempt_sec"] = (millis() - lastPublishAttemptMillis) / 1000;
        }

        doc["camera_reset"] = cameraResetCount;
        doc["capture_ms"] = lastCaptureMs;
        doc["max_capture_ms"] = maxCaptureMs;
        doc["encode_ms"] = lastEncodeMs;
        doc["mqtt_connect_ms"] = mqtt_connect_ms;
        doc["wifi_connect_ms"] = wifi_connect_ms;
        doc["camera_init_ms"] = camera_init_ms;
        doc["transport_slow"] = transport_slow;
        doc["average_jpeg_size"] = average_jpeg_size;
        doc["maximum_jpeg_size"] = maximum_jpeg_size;

        doc["suspend_remaining_sec"] = (suspend_capture_until > millis()) ? (suspend_capture_until - millis()) / 1000 : 0;
        doc["camera_consecutive_fails"] = camera_consecutive_fails;
        doc["connect_dns_ms"] = connect_dns_ms;
        doc["connect_ws_ms"] = connect_ws_ms;
        doc["connect_mqtt_ms"] = connect_mqtt_ms;

        doc["watchdog_trigger_count"] = watchdog_trigger_count;
        doc["watchdog_last_reason"] = (const char*)watchdog_last_reason;
        doc["last_watchdog_trigger_ms"] = last_watchdog_trigger_ms;
        doc["last_transport_recovery_ms"] = last_transport_recovery_ms;
        doc["transport_recovery_level"] = transport_recovery_level;

        doc["queue_depth"] = (publishQueueTail - publishQueueHead + MAX_PUBLISH_REQUESTS) % MAX_PUBLISH_REQUESTS;
        doc["queue_peak"] = publishQueuePeak;
        doc["queue_drop"] = publishQueueDropCount;
        doc["queue_enqueue_fail"] = publishQueueOverflowCount;
    }

    size_t len = measureJson(doc);
    if (len >= sizeof(telemetry_publish_buffer)) {
        publishQueueOverflowCount++;
    }

    portENTER_CRITICAL(&telemetryPublishMux);
    serializeJson(doc, telemetry_publish_buffer, sizeof(telemetry_publish_buffer));
    enqueuePublish(topic_telemetry.c_str(), telemetry_publish_buffer, true);
    portEXIT_CRITICAL(&telemetryPublishMux);
}

bool initCamera(bool rebootOnFail) {
    unsigned long start = millis();
    camera_config_t config;
    config.ledc_channel = LEDC_CHANNEL_0;
    config.ledc_timer   = LEDC_TIMER_0;
    config.pin_d0       = Y2_GPIO_NUM;
    config.pin_d1       = Y3_GPIO_NUM;
    config.pin_d2       = Y4_GPIO_NUM;
    config.pin_d3       = Y5_GPIO_NUM;
    config.pin_d4       = Y6_GPIO_NUM;
    config.pin_d5       = Y7_GPIO_NUM;
    config.pin_d6       = Y8_GPIO_NUM;
    config.pin_d7       = Y9_GPIO_NUM;
    config.pin_xclk     = XCLK_GPIO_NUM;
    config.pin_pclk     = PCLK_GPIO_NUM;
    config.pin_vsync    = VSYNC_GPIO_NUM;
    config.pin_href     = HREF_GPIO_NUM;
    config.pin_sccb_sda = SIOD_GPIO_NUM;
    config.pin_sccb_scl = SIOC_GPIO_NUM;
    config.pin_pwdn     = PWDN_GPIO_NUM;
    config.pin_reset    = RESET_GPIO_NUM;
    config.xclk_freq_hz = 20000000;
    config.pixel_format = PIXFORMAT_JPEG;

    config.frame_size = current_frame_size;  
    config.jpeg_quality = current_jpeg_quality;            
    config.fb_count = 2;

    esp_err_t err = ESP_FAIL;
    for (int i = 0; i < 3; i++) {
        err = esp_camera_init(&config);
        if (err == ESP_OK) break;
        yield(); vTaskDelay(1000 / portTICK_PERIOD_MS);
        yield();
    }

    camera_init_ms = millis() - start;

    if (err != ESP_OK) { 
        if (rebootOnFail) {
            yield(); vTaskDelay(1000 / portTICK_PERIOD_MS);
            ESP.restart(); 
        }
        return false;
    }
    return true;
}

void resetCameraOnly() {
    if (xSemaphoreTake(fbSemaphore, portMAX_DELAY) == pdTRUE) {
        if (xSemaphoreTake(cameraMutex, portMAX_DELAY) == pdTRUE) {
            cameraResetCount++;
            lastRecoveryReason = "BUTTON";

            esp_camera_deinit();
            yield(); vTaskDelay(1000 / portTICK_PERIOD_MS);
            initCamera(false);
            xSemaphoreGive(cameraMutex);
        }
        xSemaphoreGive(fbSemaphore);
    }
}

void handleButton() {
    static bool wasPressed = false;

    bool pressed = digitalRead(BUTTON_PIN) == LOW;

    if (pressed && !wasPressed) {
        buttonPressStart = millis();
    }

    if (!pressed && wasPressed) {
        unsigned long held = millis() - buttonPressStart;

        if (held >= 10000) {
            buttonPressStart = 0;
            ESP.restart();
        } else if (held >= 7000) {
            WiFiManager wm;
            wm.resetSettings();
            buttonPressStart = 0;
            ESP.restart();
        } else if (held >= 3000) {
            buttonPressStart = 0;
            resetCameraOnly();
        }
    }

    wasPressed = pressed;
}

void connectToMqtt() {
    if (otaRunning) return; // Edit 1 & 4: Prevent any new connection attempts during OTA

    yield();
    vTaskDelay(1);

    unsigned long start = millis();

    esp_task_wdt_reset();
    unsigned long dnsStart = millis();
    IPAddress brokerIP;
    WiFi.hostByName(mqtt_broker, brokerIP);
    connect_dns_ms = millis() - dnsStart;
    esp_task_wdt_reset();

    yield();
    vTaskDelay(1 / portTICK_PERIOD_MS);

    wsClient = WebsocketsClient();

    wsClient.onEvent([](WebsocketsEvent event, String data){
        switch(event){
            case WebsocketsEvent::ConnectionOpened:
                realWsConnected = true;
                wsOpenCount++;
                lastWsConnectMillis = millis();
                break;
            case WebsocketsEvent::ConnectionClosed:
                realWsConnected = false;
                wsCloseCount++;
                strncpy(lastDisconnectReason, "WS_CLOSED", sizeof(lastDisconnectReason));
                break;
            case WebsocketsEvent::GotPing:
                break;
            case WebsocketsEvent::GotPong:
                break;
        }
    });

    wsClient.onMessage([](WebsocketsMessage msg){
        if(msg.isBinary()) {
            const auto &raw = msg.rawData();
            wsWrapper.pushData(
                (const uint8_t*)raw.data(),
                raw.size()
            );
        }
    });

    String wsUrl = String("wss://") + mqtt_broker + mqtt_path;

    yield();
    vTaskDelay(1);

    esp_task_wdt_reset();

    wsClient.setInsecure();
    wsClient.addHeader("Origin", "https://broker.miot-its.org");
    wsClient.addHeader("Sec-WebSocket-Protocol", "mqtt");

    unsigned long wsStart = millis();
    bool wsOk = wsClient.connect(wsUrl);
    connect_ws_ms = millis() - wsStart;
    esp_task_wdt_reset();

    yield();
    vTaskDelay(1 / portTICK_PERIOD_MS);

    if (wsOk) {
        mqttClient.setKeepAlive(120);
        mqttClient.setSocketTimeout(5);
        
        yield();
        vTaskDelay(1 / portTICK_PERIOD_MS);

        esp_task_wdt_reset();
        unsigned long mqttStart = millis();
        bool mqttOk = mqttClient.connect(
            device_id,
            mqtt_user,
            mqtt_pass
        );
        connect_mqtt_ms = millis() - mqttStart;
        esp_task_wdt_reset();

        yield();
        vTaskDelay(1 / portTICK_PERIOD_MS);

        if (mqttOk) {
            bool ok1 = mqttClient.subscribe(topic_ota.c_str());
            bool ok2 = mqttClient.subscribe(topic_config.c_str());
            if (!(ok1 && ok2)) {
                mqttClient.disconnect();
                mqttIsConnected = false;
                mqttState = mqttClient.state();
                last_connect_result = false;
                updateTransportState(STATE_CONNECT_FAILED, REC_NONE, RecoveryReason::NONE);
                return;
            }

            mqtt_connect_ms = millis() - start;
            if (!firstMqttConnect && strcmp(lastDisconnectReason, "NONE") != 0) {
                mqttReconnectCount++;
            }
            firstMqttConnect = false;
            lastMqttConnectMillis = millis();
            mqttIsConnected = true;
            mqttState = mqttClient.state();
            last_connect_result = true;
            updateTransportState(STATE_CONNECTED, REC_NONE, RecoveryReason::NONE);

            enqueuePublish(topic_status.c_str(), "online", true);
            sendTelemetry();
            strncpy(lastDisconnectReason, "NONE", sizeof(lastDisconnectReason));
            return;
        }
    }
    mqtt_connect_ms = millis() - start;
    mqttIsConnected = false;
    mqttState = mqttClient.state();
    last_connect_result = false;
    updateTransportState(STATE_CONNECT_FAILED, REC_NONE, RecoveryReason::NONE);
}

void autoRecoverCamera() {
    if (otaRunning) return; // Edit 1: Suspend camera recovery during OTA
    static uint32_t baselineFbNull = 0;
    if (fbNullCount > baselineFbNull && millis() - lastCameraRecovery > 60000) {
        if (xSemaphoreTake(fbSemaphore, portMAX_DELAY) == pdTRUE) {
            if (xSemaphoreTake(cameraMutex, portMAX_DELAY) == pdTRUE) {
                cameraResetCount++;
                lastRecoveryReason = "AUTO_NO_IMAGE";

                esp_camera_deinit();
                yield(); vTaskDelay(1000 / portTICK_PERIOD_MS);
                initCamera(false);

                lastCameraRecovery = millis();
                baselineFbNull = fbNullCount;
                xSemaphoreGive(cameraMutex);
            }
            xSemaphoreGive(fbSemaphore);
        }
    } else {
        bool recoverMqtt = false;
        if (lastPublishSuccessMillis > 0 &&
            millis() - lastPublishSuccessMillis > 90000 &&
            mqttIsConnected &&
            realWsConnected &&
            (lastTransportRecoveryMillis == 0 || millis() - lastTransportRecoveryMillis > 180000)) {
            recoverMqtt = true;
            strncpy(lastDisconnectReason, "AUTO_TRANSPORT_RECOVERY", sizeof(lastDisconnectReason));
            transportRecoveryCount++;
        }

        if (recoverMqtt) {
            // Repeated transport recoveries within 10 minutes escalates (Requirement 4)
            static unsigned long last_transport_recovery_time = 0;
            if (last_transport_recovery_time > 0 && (millis() - last_transport_recovery_time < 600000UL)) {
                requestRecovery("REPEATED_TRANSPORT_RECOVERY");
            }
            last_transport_recovery_time = millis();

            if (transportRecoveryCount > MAX_TRANSPORT_RECOVERY) {
                if (canPerformReboot()) {
                    yield(); vTaskDelay(1000 / portTICK_PERIOD_MS);
                    ESP.restart();
                } else {
                    requestRecovery("TRANSPORT_RECOVERY_EXHAUSTED");
                    transportRecoveryCount = 0;
                }
            }

            mqttDisconnectRequested = true;
            lastTransportRecoveryMillis = millis();
        }
    }
}

void deadmanRestart(const char* reason) {
    updateTransportState(STATE_DISCONNECTED, REC_REBOOT, RecoveryReason::DEADMAN_TIMEOUT);
    requestRecovery(reason);
}

void setup() {
    cameraMutex = xSemaphoreCreateMutex();
    fbSemaphore = xSemaphoreCreateMutex();
    // Edit 2 & 3: Initialize FreeRTOS Event Group for task synchronization
    otaSyncEventGroup = xEventGroupCreate();
    if (otaSyncEventGroup != nullptr) {
        xEventGroupSetBits(otaSyncEventGroup, CAMERA_CAPTURE_IDLE_BIT | PUBLISHER_IDLE_BIT);
    }
#if defined(ESP_ARDUINO_VERSION_MAJOR) && ESP_ARDUINO_VERSION_MAJOR >= 3
    esp_task_wdt_config_t wdt_config = {
        .timeout_ms = HW_WDT_SEC * 1000,
        .idle_core_mask = (1 << portNUM_PROCESSORS) - 1,
        .trigger_panic = true
    };
    esp_task_wdt_reconfigure(&wdt_config);
#else
    esp_task_wdt_init(HW_WDT_SEC, true);
#endif

    pinMode(BUTTON_PIN, INPUT_PULLUP);
    pinMode(LED_PIN, OUTPUT);
    digitalWrite(LED_PIN, HIGH); 

    // Initialize base64 buffer in PSRAM if available, or normal RAM
    base64_buffer_size = 128 * 1024;
    if (psramFound()) {
        base64_buffer = (char*)ps_malloc(base64_buffer_size);
    }
    if (!base64_buffer) {
        base64_buffer = (char*)malloc(base64_buffer_size);
    }
    pendingFrame.base64 = base64_buffer;

    unsigned long wifiStart = millis();
    WiFiManager wm;
    wm.autoConnect("CCTV-MEDIUM-WS");
    wifi_connect_ms = millis() - wifiStart;

    initCamera(true);
    
    if (!mqttClient.setBufferSize(65536)) {
        mqttClient.setBufferSize(32768);
    }
    mqttBufferSize = mqttClient.getBufferSize();
    mqttClient.setCallback(mqttCallback);

    // vTaskPrioritySet(NULL, 1); // loop() task is priority 3 (highest)

    // CameraCaptureTask: Core 1, priority 1 (lowest)
    xTaskCreatePinnedToCore(
        CameraCaptureTask,
        "CameraCaptureTask",
        4096,
        NULL,
        1,
        &cameraTaskHandle,
        1
    );

    // ImagePublisherTask: Core 0, priority 2 (medium)
    xTaskCreatePinnedToCore(
        ImagePublisherTask,
        "ImagePublisherTask",
        4096,
        NULL,
        2,
        &publisherTaskHandle,
        0
    );

    esp_task_wdt_add(NULL);
}

void loop() {
    esp_task_wdt_reset();
    loopCounter++;
    handleButton();

    if (remoteCameraReinitRequested) {
        remoteCameraReinitRequested = false;
        resetCameraOnly();
        lastRecoveryReason = "REMOTE_CAMERA_REINIT";
    }

    if (remoteRestartRequested) {
        remoteRestartRequested = false;
        lastRecoveryReason = "REMOTE_RESTART";
        ESP.restart();
    }

    // Check for deferred recovery requests
    if (recoveryRequestReason != nullptr) {
        const char* reason = (const char*)recoveryRequestReason;
        recoveryRequestReason = nullptr;
        handleSoftRecovery(reason);
    }

    // Track MQTT unavailability duration
    if (mqttIsConnected) {
        mqtt_unavailable_start = 0;
    } else if (mqtt_unavailable_start == 0) {
        mqtt_unavailable_start = millis();
    }

    // Failure decay
    if (consecutive_failures > 0) {
        if (last_failure_free_time == 0) {
            last_failure_free_time = millis();
        } else if (millis() - last_failure_free_time >= 300000UL) {
            consecutive_failures--;
            last_failure_free_time = millis();
        }
    } else {
        last_failure_free_time = 0;
    }

    // Level 5 suspension expiration handling
    if (suspend_capture_until > 0 && millis() >= suspend_capture_until) {
        suspend_capture_until = 0;
        if (consecutive_failures == 5) {
            consecutive_failures = 2; // reset to 2 instead of keeping 5
            last_failure_free_time = millis();
        }
    }

    autoRecoverCamera();

    if (publisherStarted && millis() - publisherProgressMillis > 30000) {
        lastRecoveryReason = "PUBLISHER_STUCK";
        updateTransportState(STATE_DISCONNECTED, REC_REBOOT, RecoveryReason::PUBLISHER_STUCK);
        ESP.restart();
    }

    if (lastPublishSuccessMillis > 0 &&
        millis() - lastPublishSuccessMillis > DEADMAN_TIMEOUT_MS) {
        deadmanRestart("NO_IMAGE_SUCCESS");
    }

    if (otaRequested) {
        if (millis() - otaRequestTimestamp > 300000) {
            otaRequested = false;
            publishOtaStatus("OTA_FAILED", -1, "OTA request expired");
        } else {
            // Edit 2 & 3: Stop CameraCaptureTask and wait for both camera and publisher image pipelines to become completely idle
            otaRunning = true; // Signal tasks to stop capturing / queueing new frames
            
            bool idle = false;
            if (otaSyncEventGroup) {
                // RTOS-native non-busy wait for both capture task and publisher task to yield and report idle state
                EventBits_t bits = xEventGroupWaitBits(
                    otaSyncEventGroup,
                    CAMERA_CAPTURE_IDLE_BIT | PUBLISHER_IDLE_BIT,
                    pdFALSE,             // Do not clear bits on exit
                    pdTRUE,              // Wait for both tasks to report idle
                    pdMS_TO_TICKS(10000) // Timeout 10 seconds
                );
                
                // Confirm both tasks are idle and the pending frame buffer is empty
                if ((bits & (CAMERA_CAPTURE_IDLE_BIT | PUBLISHER_IDLE_BIT)) == (CAMERA_CAPTURE_IDLE_BIT | PUBLISHER_IDLE_BIT) &&
                    pendingFrame.state == PendingFrameState::EMPTY) {
                    idle = true;
                }
            } else {
                // Fallback in case Event Group creation failed
                idle = (pendingFrame.state == PendingFrameState::EMPTY);
            }
            
            if (idle) {
                startOta();
            } else {
                otaRunning = false;
                otaRequested = false;
                publishOtaStatus("OTA_FAILED", -1, "Pipeline idle handshake timeout");
            }
        }
    }

    if (otaRunning) {
        if (WiFi.status() != WL_CONNECTED) {
            // Edit 1: If WiFi is disconnected during OTA, fail/abort the update immediately to resume normal operation
            cleanupAndFail(otaHttp, "WiFi connection lost during OTA");
        } else {
            tickOtaStateMachine();
        }
    }

    if (!otaRunning && telemetry_enabled && (millis() - last_telemetry_time >= telemetry_interval_ms)) {
        last_telemetry_time = millis();
        sendTelemetry();
    }

    static wl_status_t prevWifi = WL_CONNECTED;
    static unsigned long wifiDisconnectTime = 0;

    if (WiFi.status() != WL_CONNECTED) {
        if (prevWifi == WL_CONNECTED) {
            wifiDropCount++;
            wifiDisconnectTime = millis();
        } else if (!otaRunning) { // Edit 1: Suspend WiFi recovery reconnection attempts during OTA
            unsigned long offlineTime = millis() - wifiDisconnectTime;
              if (offlineTime > 600000) {
                  updateTransportState(STATE_RECOVERING, REC_WIFI_BEGIN, RecoveryReason::WIFI_OFFLINE_600S);
                  WiFi.disconnect(true);
                  vTaskDelay(pdMS_TO_TICKS(1000));
                  WiFi.begin();
                  wifiDisconnectTime = millis();
              } else if (offlineTime > 300000) {
                  static unsigned long lastReconnectAttempt = 0;
                  if (millis() - lastReconnectAttempt > 60000) {
                      updateTransportState(STATE_RECOVERING, REC_WIFI_RECONNECT, RecoveryReason::WIFI_OFFLINE_300S);
                      WiFi.reconnect();
                      lastReconnectAttempt = millis();
                  }
              }
        }
    } else {
        if (prevWifi != WL_CONNECTED && wifiDisconnectTime > 0) {
            wifi_connect_ms = millis() - wifiDisconnectTime;
        }
        wifiDisconnectTime = 0;
    }

    prevWifi = (wl_status_t)WiFi.status();
    vTaskDelay(pdMS_TO_TICKS(1));
}

TransportHealth evaluateTransportHealth(bool success, uint32_t pubMs) {
    if (success) {
        if (consecutivePublishFail > 0) {
            consecutivePublishFail = 0;
        }
        if (pubMs > 5000) {
            consecutiveSlowPublishes++;
            if (consecutiveSlowPublishes >= 5) {
                return RECOVERY_REQUIRED;
            }
            if (consecutiveSlowPublishes >= 2) {
                return WARNING;
            }
        } else {
            if (consecutiveSlowPublishes > 0) {
                consecutiveSlowPublishes = 0;
            }
        }
    } else {
        consecutivePublishFail++;
        consecutiveSlowPublishes = 0;
        if (consecutivePublishFail >= 3) {
            return RECOVERY_REQUIRED;
        }
        if (consecutivePublishFail == 2) {
            return WARNING;
        }
    }
    return NORMAL;
}

void sendPhoto() {
    if (!image_enabled) return;
    if (!base64_buffer) {
        return;
    }
    
    // Edit 2: Mark camera capture task active in Event Group
    if (otaSyncEventGroup) {
        xEventGroupClearBits(otaSyncEventGroup, CAMERA_CAPTURE_IDLE_BIT);
    }

    bool isFree = false;
    portENTER_CRITICAL(&pendingFrameMux);
    if (pendingFrame.state == PendingFrameState::EMPTY) {
        isFree = true;
    }
    portEXIT_CRITICAL(&pendingFrameMux);
    if (!isFree) {
        if (otaSyncEventGroup) {
            xEventGroupSetBits(otaSyncEventGroup, CAMERA_CAPTURE_IDLE_BIT);
        }
        return;
    }

    camera_fb_t *fb = nullptr;
    unsigned long captureStart = millis();
    
    if (xSemaphoreTake(fbSemaphore, portMAX_DELAY) == pdTRUE) {
        if (xSemaphoreTake(cameraMutex, portMAX_DELAY) == pdTRUE) {
            fb = esp_camera_fb_get();
            xSemaphoreGive(cameraMutex);
        }
        
        if (!fb) {
            fbNullCount++;
            captureFailCount++;
            camera_consecutive_fails++;
            
            if (camera_consecutive_fails == 3) {
                if (xSemaphoreTake(cameraMutex, portMAX_DELAY) == pdTRUE) {
                    esp_camera_deinit();
                    yield(); vTaskDelay(1000 / portTICK_PERIOD_MS);
                    initCamera(false);
                    xSemaphoreGive(cameraMutex);
                }
            } else if (camera_consecutive_fails >= 10) {
                if (canPerformReboot()) {
                    yield(); vTaskDelay(1000 / portTICK_PERIOD_MS);
                    ESP.restart(); 
                } else {
                    if (xSemaphoreTake(cameraMutex, portMAX_DELAY) == pdTRUE) {
                        esp_camera_deinit();
                        yield(); vTaskDelay(1000 / portTICK_PERIOD_MS);
                        initCamera(false);
                        xSemaphoreGive(cameraMutex);
                        camera_consecutive_fails = 0; 
                    }
                }
            }
            xSemaphoreGive(fbSemaphore);
            if (otaSyncEventGroup) {
                xEventGroupSetBits(otaSyncEventGroup, CAMERA_CAPTURE_IDLE_BIT);
            }
            return;
        }
    } else {
        if (otaSyncEventGroup) {
            xEventGroupSetBits(otaSyncEventGroup, CAMERA_CAPTURE_IDLE_BIT);
        }
        return;
    }
    camera_consecutive_fails = 0; 

    captureOkCount++;
    lastCaptureSuccessMillis = millis();
    frameSize = fb->len;
    if (frameSize > maxFrameSize) {
        maxFrameSize = frameSize;
    }

    if (average_jpeg_size == 0.0) {
        average_jpeg_size = fb->len;
    } else {
        average_jpeg_size = (0.9 * average_jpeg_size) + (0.1 * fb->len);
    }
    if (fb->len > maximum_jpeg_size) {
        maximum_jpeg_size = fb->len;
    }

    unsigned long encodeStart = millis();
    size_t dataSize = 0;
    bool encodeSuccess = encode_base64(fb->buf, fb->len, base64_buffer, base64_buffer_size, dataSize);
    lastEncodeMs = millis() - encodeStart;

    size_t localJpegSize = fb->len;

    if (xSemaphoreTake(cameraMutex, portMAX_DELAY) == pdTRUE) {
        esp_camera_fb_return(fb);
        xSemaphoreGive(cameraMutex);
    }
    xSemaphoreGive(fbSemaphore);

    if (!encodeSuccess) {
        if (otaSyncEventGroup) {
            xEventGroupSetBits(otaSyncEventGroup, CAMERA_CAPTURE_IDLE_BIT);
        }
        return;
    }

    base64Size = dataSize;
    if (base64Size > maxBase64Size) {
        maxBase64Size = base64Size;
    }

    size_t mqttLimit = mqttBufferSize - 1024;

    if (dataSize >= mqttLimit) {
        publishFailCount++;
        requestRecovery("PAYLOAD_TOO_LARGE");
        if (otaSyncEventGroup) {
            xEventGroupSetBits(otaSyncEventGroup, CAMERA_CAPTURE_IDLE_BIT);
        }
        return;
    }

    portENTER_CRITICAL(&pendingFrameMux);
    pendingFrame.jpegSize = localJpegSize;
    pendingFrame.base64Size = dataSize;
    pendingFrame.timestamp = millis();
    pendingFrame.base64 = base64_buffer;
    pendingFrame.topic = topic_image.c_str();
    pendingFrame.state = PendingFrameState::READY;
    portEXIT_CRITICAL(&pendingFrameMux);
    if (otaSyncEventGroup) {
        xEventGroupSetBits(otaSyncEventGroup, CAMERA_CAPTURE_IDLE_BIT);
    }
}

void CameraCaptureTask(void* pvParameters) {
    esp_task_wdt_add(NULL);
    while (true) {
        esp_task_wdt_reset();
        
        if (otaRunning) {
            portENTER_CRITICAL(&pendingFrameMux);
            pendingFrame.state = PendingFrameState::EMPTY;
            portEXIT_CRITICAL(&pendingFrameMux);
            if (otaSyncEventGroup) {
                xEventGroupSetBits(otaSyncEventGroup, CAMERA_CAPTURE_IDLE_BIT);
            }
            vTaskDelay(pdMS_TO_TICKS(200));
            continue;
        }

        if (!image_enabled) {
            vTaskDelay(pdMS_TO_TICKS(200));
            continue;
        }

        if (millis() < suspend_capture_until) {
            vTaskDelay(pdMS_TO_TICKS(500));
            continue;
        }

        if (millis() - last_capture_time > capture_interval_ms) {
            last_capture_time = millis();
            
            bool isFree = false;
            portENTER_CRITICAL(&pendingFrameMux);
            if (pendingFrame.state == PendingFrameState::EMPTY) {
                isFree = true;
            }
            portEXIT_CRITICAL(&pendingFrameMux);

            if (isFree) {
                sendPhoto();
                bool isReady = false;
                portENTER_CRITICAL(&pendingFrameMux);
                if (pendingFrame.state == PendingFrameState::READY) {
                    isReady = true;
                }
                portEXIT_CRITICAL(&pendingFrameMux);

                if (isReady && publisherTaskHandle != NULL) {
                    xTaskNotifyGive(publisherTaskHandle);
                }
            }
        }
        vTaskDelay(pdMS_TO_TICKS(10));
    }
}

void ImagePublisherTask(void* pvParameters) {
    esp_task_wdt_add(NULL);
    
    static unsigned long lastMqttRetryTime = 0;
    static unsigned long mqttRetryDelayMs = 1000;

    while (true) {
        publisherStarted = true;
        publisherProgressMillis = millis();
        esp_task_wdt_reset();

        if (otaRunning) {
            portENTER_CRITICAL(&pendingFrameMux);
            pendingFrame.state = PendingFrameState::EMPTY;
            portEXIT_CRITICAL(&pendingFrameMux);
            if (otaSyncEventGroup) {
                xEventGroupSetBits(otaSyncEventGroup, PUBLISHER_IDLE_BIT);
            }
        }

        ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(100));

        // 8. Never trust only mqttClient.connected(). Health must require BOTH mqttClient.connected() AND recent successful publish within timeout.
        if (mqttClient.connected() && !otaRunning) {
            updateTransportState(STATE_CONNECTED, REC_NONE, RecoveryReason::NONE);
            
            // Skip watchdog check if we connected very recently (grace period)
            if (millis() - lastMqttConnectMillis >= 30000UL) {
                bool watchdogTriggered = false;
                const char* triggerReason = "NONE";
                if (consecutivePublishFail >= 10) {
                    watchdogTriggered = true;
                    triggerReason = "CONSECUTIVE_FAILURES";
                } else if (image_enabled && (millis() >= suspend_capture_until) && (lastPublishSuccessMillis > 0) && (millis() - lastPublishSuccessMillis > TRANSPORT_WATCHDOG_TIMEOUT_MS)) {
                    watchdogTriggered = true;
                    triggerReason = "TIMEOUT";
                }

                if (watchdogTriggered && (millis() - lastWatchdogRecoveryMillis > 60000)) {
                    mqttDisconnectRequested = true;
                    lastWatchdogRecoveryMillis = millis();
                    RecoveryReason recReason = (strcmp(triggerReason, "TIMEOUT") == 0) ? RecoveryReason::WATCHDOG_TIMEOUT : RecoveryReason::CONSECUTIVE_FAILURES;
                    updateTransportState(STATE_RECOVERING, REC_TRANSPORT_RECONNECT, recReason);
                    strncpy(lastDisconnectReason, "WATCHDOG_RECOVERY", sizeof(lastDisconnectReason));
                }
            }
        }

        if (mqttDisconnectRequested) {
            mqttDisconnectRequested = false;
            mqttClient.disconnect();
            wsClient.close();
            lastTransportRecoveryMillis = millis();
            mqttIsConnected = false;
            mqttState = mqttClient.state();
            mqttRetryDelayMs = 1000;
            consecutive_transport_recovery++;

            // Requirement 4: If transport recovery repeats 5 consecutive times without successful publish, WiFi reconnect
            if (consecutive_transport_recovery >= 5) {
                consecutive_transport_recovery = 0;
                updateTransportState(STATE_RECOVERING, REC_WIFI_RECONNECT, RecoveryReason::CONSECUTIVE_TRANSPORT_FAILURES);
                WiFi.disconnect(false);
                vTaskDelay(pdMS_TO_TICKS(100));
                WiFi.reconnect();
            } else {
                updateTransportState(STATE_RECOVERING, REC_TRANSPORT_RECONNECT, RecoveryReason::NONE);
            }
        }

        if (!mqttClient.connected()) {
            mqttIsConnected = false;
            mqttState = mqttClient.state();
            if (strcmp(transport_state, STATE_RECOVERING) != 0 && strcmp(transport_state, STATE_CONNECT_FAILED) != 0) {
                updateTransportState(STATE_DISCONNECTED, REC_NONE, RecoveryReason::NONE);
            }
            if (otaRunning) {
                // Edit 1 & 4: Suspend MQTT reconnect attempts during OTA
                if (otaSyncEventGroup) {
                    xEventGroupSetBits(otaSyncEventGroup, PUBLISHER_IDLE_BIT);
                }
                vTaskDelay(pdMS_TO_TICKS(100));
                continue;
            }
            if (WiFi.status() == WL_CONNECTED) {
                if (transport_recovery_level < 2) {
                    updateTransportState(STATE_RECOVERING, REC_MQTT_RECONNECT, RecoveryReason::MQTT_CONNECTING);
                }
                int32_t jitter = (int32_t)(esp_random() % 501) - 250;
                long currentDelay = (long)mqttRetryDelayMs + jitter;
                if (currentDelay < 0) currentDelay = 0;
                if (currentDelay > 30000) currentDelay = 30000;

                if (millis() - lastMqttRetryTime >= (unsigned long)currentDelay) {
                    lastMqttRetryTime = millis();
                    if (strcmp(lastDisconnectReason, "NONE") == 0) {
                        snprintf(lastDisconnectReason, sizeof(lastDisconnectReason), "MQTT_STATE_%d", mqttClient.state());
                    }
                    connectToMqtt();
                    if (mqttClient.connected()) {
                        mqttRetryDelayMs = 1000;
                    } else {
                        mqttRetryDelayMs = std::min(mqttRetryDelayMs * 2, 30000UL);
                    }
                }
            } else {
                strncpy(lastDisconnectReason, "WIFI_LOST", sizeof(lastDisconnectReason));
            }
        } else {
            mqttIsConnected = true;
            mqttState = mqttClient.state();
            mqttRetryDelayMs = 1000;
        }

        mqttClient.loop();
        wsClient.poll();

        int publishedCount = 0;

        // Process PendingFrame first (if ready and we have budget)
        if (!otaRunning && publishedCount < 5 && mqttClient.connected()) {
            bool imageReady = false;
            const char* img_topic = nullptr;
            char* img_payload = nullptr;
            size_t img_jpegSize = 0;
            size_t img_base64Size = 0;
            uint32_t img_timestamp = 0;

            portENTER_CRITICAL(&pendingFrameMux);
            if (pendingFrame.state == PendingFrameState::READY) {
                pendingFrame.state = PendingFrameState::PUBLISHING;
                img_topic = pendingFrame.topic;
                img_payload = pendingFrame.base64;
                img_jpegSize = pendingFrame.jpegSize;
                img_base64Size = pendingFrame.base64Size;
                img_timestamp = pendingFrame.timestamp;
                imageReady = true;
            }
            portEXIT_CRITICAL(&pendingFrameMux);

            if (imageReady) {
                // Edit 3: Mark publisher active in Event Group
                if (otaSyncEventGroup) {
                    xEventGroupClearBits(otaSyncEventGroup, PUBLISHER_IDLE_BIT);
                }
                unsigned long pubStart = millis();
                lastPublishAttemptMillis = millis();
                
                bool success = mqttClient.publish(img_topic, img_payload);
                publishedCount++;
                
                uint32_t publishMsVal = millis() - pubStart;
                publishMs = publishMsVal;
                if (publishMsVal > maxPublishMs) {
                    maxPublishMs = publishMsVal;
                }

                transport_slow = (publishMsVal > 5000);

                if (publishMsVal > capture_interval_ms) {
                    publishOverIntervalCount++;
                }

                static uint32_t consecutivePublishStall = 0;
                if (publishMsVal > 10000) {
                    publishStallCount++;
                    consecutivePublishStall++;
                    if (consecutivePublishStall >= 3) {
                        strncpy(lastDisconnectReason, "PUBLISH_STALL", sizeof(lastDisconnectReason));
                        mqttClient.disconnect();
                        wsClient.close();
                        consecutivePublishStall = 0;
                    }
                } else if (success) {
                    consecutivePublishStall = 0;
                }

                TransportHealth health = evaluateTransportHealth(success, publishMsVal);

                last_publish_result = success;
                if (success) {
                    publishOkCount++;
                    consecutive_failures = 0; 
                    lastPublishSuccessMillis = millis();
                    updateTransportState(STATE_CONNECTED, REC_NONE, RecoveryReason::NONE);
                } else {
                    publishFailCount++;
                    lastFailedPublishMqttState = mqttClient.state();
                    lastFailedPublishWifiStatus = (wl_status_t)WiFi.status();
                }

                portENTER_CRITICAL(&pendingFrameMux);
                pendingFrame.state = PendingFrameState::EMPTY;
                portEXIT_CRITICAL(&pendingFrameMux);

                mqttClient.loop();
                wsClient.poll();

                // 7. Let watchdog decide on recovery. Do not immediately reconnect.
                // if (health == RECOVERY_REQUIRED) {
                //     if (consecutivePublishFail >= 3) {
                //         requestRecovery("PUBLISH_FAILED");
                //     }
                // }
                // Edit 3: Mark publisher idle in Event Group
                if (otaSyncEventGroup) {
                    xEventGroupSetBits(otaSyncEventGroup, PUBLISHER_IDLE_BIT);
                }
            }
        } else if (!mqttClient.connected()) {
            portENTER_CRITICAL(&pendingFrameMux);
            if (pendingFrame.state == PendingFrameState::READY) {
                pendingFrame.state = PendingFrameState::EMPTY;
            }
            portEXIT_CRITICAL(&pendingFrameMux);
        }

        PublishRequest req;
        while (publishedCount < 5 && dequeuePublish(req)) {
            if (!mqttClient.connected()) {
                continue;
            }

            bool success = mqttClient.publish(req.topic, req.payload, req.retained);
            publishedCount++;
            
            last_publish_result = success;
            if (success) {
                updateTransportState(STATE_CONNECTED, REC_NONE, RecoveryReason::NONE);
            } else {
                lastFailedPublishMqttState = mqttClient.state();
                lastFailedPublishWifiStatus = (wl_status_t)WiFi.status();
                if (strcmp(req.topic, topic_telemetry.c_str()) == 0) {
                    telemetryPublishFailCount++;
                }
            }
            mqttClient.loop();
            wsClient.poll();
        }

        if (publishedCount > 0) {
            yield();
            vTaskDelay(1 / portTICK_PERIOD_MS);
        }
    }
}