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
  #define LED_PIN 4
  #define DEVICE_HOSTNAME "cctv-medium-local"
  #define FW_VERSION "1.0.1"
  #define FW_BOARD   "ESP32-CAM"
  #define FW_MODEL   "AI_THINKER"
  #define FW_BUILD   __DATE__ " " __TIME__
  #define BUTTON_PIN 13

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
  framesize_t current_frame_size = FRAMESIZE_SVGA;
  int current_jpeg_quality = 25;

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
  String lastDisconnectReason = "NONE";
  uint32_t publishStallCount = 0;
  uint32_t transportRecoveryCount = 0;
  uint32_t loopCounter = 0;
  unsigned long lastCameraRecovery = 0;

  unsigned long lastImageSuccessMillis = 0;
  unsigned long lastTransportRecoveryMillis = 0;
  unsigned long lastWsConnectMillis = 0;
  uint32_t wsOpenCount = 0;
  uint32_t wsCloseCount = 0;
  unsigned long lastPublishAttemptMillis = 0;
  uint32_t publishOverIntervalCount = 0;
  uint32_t telemetryPublishFailCount = 0;

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

  void transitionOtaState(OTAState newState) {
      Serial.printf("[OTA] Transition: %s -> %s (Version: %s)\n", otaStateToString(currentOtaState), otaStateToString(newState), FW_VERSION);
      currentOtaState = newState;
      otaRunning = (currentOtaState != OTA_IDLE && currentOtaState != OTA_SUCCESS && currentOtaState != OTA_FAILED && currentOtaState != OTA_CANCELLED);
  }

  void publishOtaStatus(const char* status, int progress = -1, const char* messageOrReason = "", const char* version = "") {
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
      
      String payload;
      serializeJson(doc, payload);
      bool ok = mqttClient.publish(topic_ota_status.c_str(), payload.c_str(), true);

      Serial.printf(
      "[OTA STATUS] topic=%s publish=%s state=%d connected=%d\n",
      topic_ota_status.c_str(),
      ok ? "OK" : "FAIL",
      mqttClient.state(),
      mqttClient.connected()
      );
  }

  void logOtaFailure() {
      double downloadDurationSec = (otaDownloadEndTime > otaDownloadStartTime) ? (otaDownloadEndTime - otaDownloadStartTime) / 1000.0 : 0.0;
      double downloadSpeed = downloadDurationSec > 0 ? (double)otaBytesDownloaded / downloadDurationSec : 0;
      unsigned long flashDuration = (otaFlashEndTime > otaFlashStartTime) ? (otaFlashEndTime - otaFlashStartTime) : 0;
      unsigned long totalDuration = (otaStartTime > 0) ? (millis() - otaStartTime) : 0;

      Serial.printf("[OTA] Failure Details:\n");
      Serial.printf("  Current State: %s\n", otaStateToString(currentOtaState));
      Serial.printf("  Target Version: %s\n", otaVersion.length() > 0 ? otaVersion.c_str() : "Unknown");
      Serial.printf("  Downloaded Bytes: %u\n", otaBytesDownloaded);
      Serial.printf("  Download Speed: %.2f bytes/sec\n", downloadSpeed);
      Serial.printf("  Flash Duration: %lu ms\n", flashDuration);
      Serial.printf("  Total Duration: %lu ms\n", totalDuration);
      Serial.printf("  Reason on Failure: %s\n", otaFailReason.c_str());
  }

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
      logOtaFailure();
      resetOtaState();
  }

  bool downloadManifest(String& manifestContent) {
      int retryCount = 0;
      int backoffMs = 1000;
      WiFiClientSecure client;
      client.setInsecure();
      HTTPClient http;
      http.setTimeout(30000);
      http.setFollowRedirects(HTTPC_FORCE_FOLLOW_REDIRECTS);
      
      while (retryCount < 3) {
          if (otaCancelled) {
              http.end();
              return false;
          }
          
          Serial.printf("[OTA] Downloading manifest (Attempt %d/3) from: %s\n", retryCount + 1, otaRequestManifestUrl.c_str());
          if (http.begin(client, otaRequestManifestUrl)) {
              int httpCode = http.GET();
              if (httpCode == HTTP_CODE_OK) {
                  manifestContent = http.getString();
                  http.end();
                  return true;
              }
              Serial.printf("[OTA] Manifest GET failed, HTTP: %d\n", httpCode);
              http.end();
          } else {
              Serial.println("[OTA] Manifest http.begin failed");
          }
          
          retryCount++;
          if (retryCount < 3) {
              Serial.printf("[OTA] Retrying in %d ms...\n", backoffMs);
              unsigned long waitStart = millis();
              while (millis() - waitStart < (unsigned long)backoffMs) {
                  mqttClient.loop();
                  wsClient.poll();
                  yield();
              }
              backoffMs *= 2;
          }
      }
      
      otaFailReason = "Manifest download failed after 3 retries";
      return false;
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
      if (otaSha256.length() == 0) {
          Serial.println("[OTA] WARNING: SHA256 checksum absent in manifest. Continuing without hash verification.");
      }
      return true;
  }

  bool downloadFirmware(HTTPClient& http, WiFiClientSecure& client) {
        int retryCount = 0;
        int backoffMs = 1000;
        client.setInsecure();
        http.setTimeout(120000);
        http.setFollowRedirects(HTTPC_FORCE_FOLLOW_REDIRECTS);
        
        const char* headerKeys[] = {"Content-Type"};
        http.collectHeaders(headerKeys, 1);
        
        while (retryCount < 3) {
            if (otaCancelled) return false;
          
        Serial.printf("[OTA] Connecting to firmware (Attempt %d/3) at: %s\n",
            retryCount + 1,
            otaUrl.c_str()
        );

        if (http.begin(client, otaUrl)) {

            int httpCode = http.GET();

            if (httpCode == HTTP_CODE_OK) {

                int totalLength = http.getSize();

                Serial.printf(
                    "[OTA] HTTP=%d Content-Type=%s Size=%d Expected=%u\n",
                    httpCode,
                    http.header("Content-Type").c_str(),
                    totalLength,
                    otaSize
                );

                if (totalLength > 0 && (size_t)totalLength == otaSize) {
                    return true;
                }

                otaFailReason = "Firmware size mismatch";
                Serial.printf(
                    "[OTA] Size mismatch. HTTP=%d Expected=%u\n",
                    totalLength,
                    otaSize
                );

                http.end();
            }
            else {

                otaFailReason = "HTTP " + String(httpCode);

                Serial.printf(
                    "[OTA] Firmware GET failed HTTP=%d\n",
                    httpCode
                );

                http.end();
            }

        }
        else {

            otaFailReason = "http.begin failed";

            Serial.println("[OTA] Firmware http.begin failed");

        }
          
        retryCount++;
        if (retryCount < 3) {
            Serial.printf("[OTA] Retrying in %d ms...\n", backoffMs);
            unsigned long waitStart = millis();
            while (millis() - waitStart < (unsigned long)backoffMs) {
                mqttClient.loop();
                wsClient.poll();
                yield();
            }
            backoffMs *= 2;
        }
      }
      
      otaFailReason = "Firmware download failed after 3 retries";
      return false;
  }

  bool flashFirmware(WiFiClient* stream, SHA256Hasher& hasher) {
        otaFlashStartTime = millis();
        const esp_partition_t *running = esp_ota_get_running_partition();
        const esp_partition_t *next = esp_ota_get_next_update_partition(NULL);

        Serial.printf("Running partition : %s\n", running ? running->label : "NULL");
        Serial.printf("Next partition    : %s\n", next ? next->label : "NULL");

        if (running) {
            Serial.printf("Running subtype=%d addr=0x%08X size=%u\n",
                running->subtype,
                running->address,
                running->size);
        }

        if (next) {
            Serial.printf("Next subtype=%d addr=0x%08X size=%u\n",
                next->subtype,
                next->address,
                next->size);
        }

        Serial.printf("FreeSketch=%u OTA=%u\n",
            ESP.getFreeSketchSpace(),
            otaSize);
        if (!Update.begin(otaSize, U_FLASH)) {
            otaFailReason = "Update.begin failed: " + String(Update.errorString());
            return false;
        }
      
        size_t written = 0;
        static uint8_t buff[1024];
        int lastProgress = 0;
        unsigned long lastHeartbeatTime = 0;
        unsigned long lastDataMillis = millis();
      
        while (written < otaSize) {
            mqttClient.loop();
            wsClient.poll();
            yield();

            if (otaCancelled) {
                otaFailReason = "OTA_CANCELLED";
                Update.abort();
                return false;
            }

            if (millis() - otaFlashStartTime > 120000) {
                otaFailReason = "Flash timeout (180s)";
                Update.abort();
                return false;
            }

            if (millis() - lastDataMillis > 60000) {
                otaFailReason = "Download stalled";
                Update.abort();
                return false;
            }

            if (millis() - lastHeartbeatTime >= 5000) {
                lastHeartbeatTime = millis();
                int progressVal = (written * 100) / otaSize;
                publishOtaStatus("OTA_RUNNING", progressVal);
            }

            size_t available = stream->available();

            if (available > 0) {
                size_t toRead = std::min(available, sizeof(buff));
                int readBytes = stream->readBytes(buff, toRead);
                if (readBytes <= 0) {
                    Serial.printf("[OTA] readBytes=%d available=%u\n",
                                readBytes,
                                (unsigned)available);
                }
                if (readBytes > 0) {
                    lastDataMillis = millis();

                    if (otaSha256.length() > 0) {
                        hasher.update(buff, readBytes);
                    }

                    size_t w = Update.write(buff, readBytes);

                    if (w != (size_t)readBytes) {
                        otaFailReason = "Flash write failed";
                        Update.abort();
                        return false;
                    }

                    written += readBytes;
                    otaBytesDownloaded = written;

                    int progress = (written * 100) / otaSize;
                    int progressTen = (progress / 10) * 10;

                    if (progressTen > lastProgress) {
                        lastProgress = progressTen;
                        publishOtaStatus("OTA_PROGRESS", lastProgress);
                    }
                }
            } else {
                static unsigned long lastLog = 0;
                if (millis() - lastLog >= 1000) {
                    lastLog = millis();
                    Serial.printf(
                        "[OTA WAIT] written=%u/%u available=%u connected=%d\n",
                        (unsigned)written,
                        (unsigned)otaSize,
                        (unsigned)available
                    );
                }
                delay(10);
            }
        }
      otaDownloadEndTime = millis();
      return true;
  }

  bool verifyFirmware(SHA256Hasher& hasher) {
      transitionOtaState(OTA_VERIFY);
      publishOtaStatus("OTA_VERIFY");
      
      if (otaSha256.length() > 0) {
          String calculatedSha = hasher.finish();
          if (calculatedSha != otaSha256) {
              otaFailReason = "SHA256 mismatch";
              Update.abort();
              return false;
          }
      } else {
          Serial.println("[OTA] Checksum absent. Skipping SHA256 verification.");
      }
      
      otaFlashEndTime = millis();
      if (!Update.end(true)) {
          otaFailReason = "Update.end failed: " + String(Update.errorString());
          return false;
      }
      
      return true;
  }

  void performOTA() {
      otaRequested = false;
      otaRunning = true;
      otaStartTime = millis();
      otaBytesDownloaded = 0;
      
      transitionOtaState(OTA_DOWNLOAD_MANIFEST);
      publishOtaStatus("OTA_START");
      
      String manifestContent;
      if (!downloadManifest(manifestContent)) {
          HTTPClient dummyHttp;
          if (otaCancelled) {
              handleOtaCancel(dummyHttp);
          } else {
              cleanupAndFail(dummyHttp, otaFailReason);
          }
          return;
      }
      
      transitionOtaState(OTA_VALIDATE);
      if (!parseManifest(manifestContent) || !validateManifest()) {
          HTTPClient dummyHttp;
          cleanupAndFail(dummyHttp, otaFailReason);
          return;
      }
      
      transitionOtaState(OTA_DOWNLOAD_FIRMWARE);
      WiFiClientSecure client;
      HTTPClient http;
      if (!downloadFirmware(http, client)) {
          if (otaCancelled) {
              handleOtaCancel(http);
          } else {
              cleanupAndFail(http, otaFailReason);
          }
          return;
      }
      
      transitionOtaState(OTA_FLASH);
      publishOtaStatus("OTA_FLASH");
      
      WiFiClient* stream = http.getStreamPtr();
      SHA256Hasher hasher;
      otaDownloadStartTime = millis();
      if (!flashFirmware(stream, hasher)) {
          if (otaCancelled) {
              handleOtaCancel(http);
          } else {
              cleanupAndFail(http, otaFailReason);
          }
          return;
      }
      http.end();
      
      if (!verifyFirmware(hasher)) {
          cleanupAndFail(http, otaFailReason);
          return;
      }
      
      transitionOtaState(OTA_SUCCESS);
      
      double downloadDurationSec = (otaDownloadEndTime - otaDownloadStartTime) / 1000.0;
      double downloadSpeed = downloadDurationSec > 0 ? (double)otaSize / downloadDurationSec : 0;
      unsigned long flashDuration = otaFlashEndTime - otaFlashStartTime;
      unsigned long totalDuration = millis() - otaStartTime;
      
      Serial.printf("[OTA] Success Details:\n");
      Serial.printf("  Version: %s\n", otaVersion.c_str());
      Serial.printf("  Download Size: %u bytes\n", otaSize);
      Serial.printf("  Download Speed: %.2f bytes/sec\n", downloadSpeed);
      Serial.printf("  Flash Duration: %lu ms\n", flashDuration);
      Serial.printf("  Total Duration: %lu ms\n", totalDuration);
      
      publishOtaStatus("OTA_SUCCESS", 100, "Firmware updated.", otaVersion.c_str());
      
      resetOtaState();
      delay(2000);
      ESP.restart();
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

  bool initCamera(bool rebootOnFail = true);

  void publishConfigStatus(const char* status, const char* messageOrReason, bool success) {
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
      
      String payload;
      serializeJson(doc, payload);
      bool ok = mqttClient.publish(topic_config_status.c_str(), payload.c_str(), true);

      Serial.printf(
          "[CONFIG STATUS] topic=%s publish=%s state=%d connected=%d\n",
          topic_config_status.c_str(),
          ok ? "OK" : "FAIL",
          mqttClient.state(),
          mqttClient.connected()
      );
  }

  void handleConfigPayload(byte* payload, unsigned int length) {
      Serial.println("[CONFIG] REQUEST RECEIVED");
      #if ARDUINOJSON_VERSION_MAJOR >= 7
        JsonDocument doc;
      #else
        DynamicJsonDocument doc(1024);
      #endif
      
      DeserializationError error = deserializeJson(doc, (const char*)payload, length);
      if (error) {
          Serial.printf("[CONFIG] Parse error: %s\n", error.c_str());
          publishConfigStatus("CONFIG_FAILED", "Malformed JSON", false);
          return;
      }
      
      Serial.println("[CONFIG] JSON OK");

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
      String new_frame_size_str = frameSizeToString(current_frame_size);
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
          new_frame_size_str = val;
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

      // Log the apply actions
      if (cfg.containsKey("frame_size")) {
          Serial.printf("[CONFIG] APPLY frame_size=%s\n", new_frame_size_str.c_str());
      }
      if (cfg.containsKey("jpeg_quality")) {
          Serial.printf("[CONFIG] APPLY jpeg_quality=%d\n", new_jpeg_quality);
      }
      if (cfg.containsKey("capture_interval_ms")) {
          Serial.printf("[CONFIG] APPLY capture_interval_ms=%lu\n", new_capture_interval_ms);
      }
      if (cfg.containsKey("telemetry_interval_ms")) {
          Serial.printf("[CONFIG] APPLY telemetry_interval_ms=%lu\n", new_telemetry_interval_ms);
      }
      if (cfg.containsKey("image_enabled")) {
          Serial.printf("[CONFIG] APPLY image_enabled=%s\n", new_image_enabled ? "true" : "false");
      }
      if (cfg.containsKey("telemetry_enabled")) {
          Serial.printf("[CONFIG] APPLY telemetry_enabled=%s\n", new_telemetry_enabled ? "true" : "false");
      }
      if (cfg.containsKey("ota_enabled")) {
          Serial.printf("[CONFIG] APPLY ota_enabled=%s\n", new_ota_enabled ? "true" : "false");
      }
      if (cfg.containsKey("mqtt_buffer")) {
          Serial.printf("[CONFIG] APPLY mqtt_buffer=%d\n", new_mqtt_buffer);
      }

      // Apply camera configuration (safe camera re-init if changed)
      bool cameraChanged = (new_frame_size != current_frame_size || new_jpeg_quality != current_jpeg_quality);
      if (cameraChanged) {
          framesize_t old_frame_size = current_frame_size;
          int old_jpeg_quality = current_jpeg_quality;
          
          current_frame_size = new_frame_size;
          current_jpeg_quality = new_jpeg_quality;
          
          esp_camera_deinit();
          delay(1000);
          if (!initCamera(false)) {
              // Rollback
              current_frame_size = old_frame_size;
              current_jpeg_quality = old_jpeg_quality;
              esp_camera_deinit();
              delay(1000);
              initCamera(false);
              publishConfigStatus("CONFIG_FAILED", "Camera re-init failed", false);
              return;
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

      Serial.println("[CONFIG] SUCCESS");
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
              Serial.printf("[OTA] Parse error: %s\n", error.c_str());
              return;
          }
          
          if (!doc.containsKey("action")) {
              return;
          }
          
          String action = doc["action"].as<String>();

          if (action == "ota") {
              Serial.println("[OTA] OTA REQUEST RECEIVED");
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
              Serial.printf("[OTA] Manifest=%s\n", otaRequestManifestUrl.c_str());
              otaRequested = true;
              otaRequestTimestamp = millis();
          } else if (action == "cancel") {
              if (otaRunning) {
                  otaCancelled = true;
              } else {
                  publishOtaStatus("OTA_FAILED", -1, "No OTA running");
              }
          } else if (action == "check") {
              Serial.println("[OTA] CHECK REQUEST");
              #if ARDUINOJSON_VERSION_MAJOR >= 7
                JsonDocument checkDoc;
              #else
                StaticJsonDocument<128> checkDoc;
              #endif
              checkDoc["status"] = "OTA_CHECK";
              checkDoc["version"] = FW_VERSION;
              String checkPayload;
              serializeJson(checkDoc, checkPayload);
              bool ok = mqttClient.publish(
                  topic_ota_status.c_str(),
                  checkPayload.c_str(),
                  true
              );

              Serial.printf(
              "[OTA CHECK] topic=%s publish=%s state=%d connected=%d\n",
              topic_ota_status.c_str(),
              ok ? "OK" : "FAIL",
              mqttClient.state(),
              mqttClient.connected()
              );
          }
      } else if (String(topic) == topic_config) {
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

  void sendTelemetry() {
    if (!telemetry_enabled) return;
    if (otaRunning) return;

  #if ARDUINOJSON_VERSION_MAJOR >= 7
    JsonDocument doc;
  #else
    StaticJsonDocument<512> doc;
  #endif
    doc["device_id"] = device_id;
    doc["fw_version"] = FW_VERSION;
    doc["fw_board"] = FW_BOARD;
    doc["fw_model"] = FW_MODEL;
    doc["fw_build"] = FW_BUILD;
    doc["ota_supported"] = true;
    doc["ota_running"] = otaRunning;
    doc["free_ota_space"] = ESP.getFreeSketchSpace();
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
    if (psramFound()) {
      doc["free_psram"] = ESP.getFreePsram();
    }
    uint32_t largestBlock = heap_caps_get_largest_free_block(MALLOC_CAP_8BIT);
    doc["largest_free_block"] = largestBlock;

    uint32_t freeHeap = ESP.getFreeHeap();
    uint32_t fragmentationPct = 0;
    if (freeHeap > 0 && largestBlock <= freeHeap)
    {
        fragmentationPct = 100 - ((largestBlock * 100) / freeHeap);
    }
    doc["fragmentation_pct"] = fragmentationPct;
    doc["rssi"] = WiFi.RSSI();
    doc["wifi_status"] = WiFi.status();

    doc["mqtt_connected"] = mqttClient.connected();
    doc["ws_connected"] = realWsConnected;
    doc["ws_uptime_sec"] = realWsConnected ? (millis() - lastWsConnectMillis) / 1000 : 0;
    doc["ws_open_count"] = wsOpenCount;
    doc["ws_close_count"] = wsCloseCount;
    doc["mqtt_state"] = mqttClient.state();
    doc["mqtt_uptime_sec"] = mqttClient.connected() ? (millis() - lastMqttConnectMillis) / 1000 : 0;
    doc["loop_counter"] = loopCounter;

    doc["capture_ok"] = captureOkCount;
    doc["capture_fail"] = captureFailCount;

    doc["publish_ms"] = publishMs;
    doc["max_publish_ms"] = maxPublishMs;
    doc["frame_size"] = frameSize;
    doc["base64_size"] = base64Size;
    doc["max_base64_size"] = maxBase64Size;
    doc["mqtt_buffer"] = mqttBufferSize;
    doc["max_frame_size"] = maxFrameSize;

    doc["publish_ok"] = publishOkCount;
    doc["publish_fail"] = publishFailCount;
    doc["telemetry_publish_fail"] = telemetryPublishFailCount;
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
    doc["wifi_drop"] = wifiDropCount;
    doc["mqtt_reconnect"] = mqttReconnectCount;
    doc["recovery_reason"] = lastRecoveryReason;
    doc["disconnect_reason"] = lastDisconnectReason;
    if (lastImageSuccessMillis == 0) {
      doc["last_image_sec"] = -1;
    } else {
      doc["last_image_sec"] =
        (millis() - lastImageSuccessMillis) / 1000;
    }

    doc["reset_reason"] = getResetReason();

    String payload;
    serializeJson(doc, payload);

    if (mqttClient.connected()) {

      bool telemetryOk = mqttClient.publish(
        topic_telemetry.c_str(),
        payload.c_str(),
        true
      );

      if (!telemetryOk) {
          telemetryPublishFailCount++;
      }
    }
  }

  bool initCamera(bool rebootOnFail) {
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

    esp_err_t err = esp_camera_init(&config);
    if (err != ESP_OK) { 
        if (rebootOnFail) {
            ESP.restart(); 
        }
        return false;
    }
    return true;
  }
  void resetCameraOnly()
  {
      cameraResetCount++;
      lastRecoveryReason = "BUTTON";

      esp_camera_deinit();

      delay(1000);

      initCamera();
  }
  void handleButton()
  {
      static bool wasPressed = false;

      bool pressed =
          digitalRead(BUTTON_PIN) == LOW;

      if (pressed && !wasPressed)
      {
          buttonPressStart = millis();
      }

      if (!pressed && wasPressed)
      {
          unsigned long held =
              millis() - buttonPressStart;

          if (held >= 10000)
          {
              buttonPressStart = 0;
              ESP.restart();
          }
          else if (held >= 5000)
          {

              WiFiManager wm;
              wm.resetSettings();
              buttonPressStart = 0;
              ESP.restart();
          }
          else if (held >= 3000)
          {   
              buttonPressStart = 0;
              resetCameraOnly();
          }
      }

      wasPressed = pressed;
  }
  unsigned long lastMqttRetry = 0;
  void connectToMqtt() {

    if (millis() - lastMqttRetry < 5000) return;
    lastMqttRetry = millis();

      IPAddress brokerIP;
      WiFi.hostByName(mqtt_broker, brokerIP);

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
            lastDisconnectReason = "WS_CLOSED";
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

      String wsUrl =
          String("wss://") +
          mqtt_broker +
          mqtt_path;

      bool wsOk = wsClient.connect(wsUrl);

      if (wsOk) {

        mqttClient.setKeepAlive(120);
        mqttClient.setSocketTimeout(5);
        bool mqttOk = mqttClient.connect(
          device_id,
          mqtt_user,
          mqtt_pass
        );

        if (mqttOk) {
          if (!firstMqttConnect && lastDisconnectReason != "NONE")
          {
              mqttReconnectCount++;
          }
          firstMqttConnect = false;
          lastMqttConnectMillis = millis();
          lastDisconnectReason = "NONE";

          mqttClient.publish(
            topic_status.c_str(),
            "online",
            true
          );
          bool subOk = mqttClient.subscribe(topic_ota.c_str());
          bool subConfigOk = mqttClient.subscribe(topic_config.c_str());
          sendTelemetry();

          return;
        }
      }
  }
  void autoRecoverCamera()
  {
      static uint32_t baselineFbNull = 0;
      if (
          fbNullCount > baselineFbNull &&
          millis() - lastCameraRecovery > 60000
      )
      {
          cameraResetCount++;
          lastRecoveryReason = "AUTO_NO_IMAGE";

          esp_camera_deinit();

          delay(1000);

          initCamera();

          lastCameraRecovery = millis();
          baselineFbNull = fbNullCount;
      }
      else if (
        lastImageSuccessMillis > 0 &&
        millis() - lastImageSuccessMillis > 90000 &&
        mqttClient.connected() &&
        realWsConnected &&
        (lastTransportRecoveryMillis == 0 || millis() - lastTransportRecoveryMillis > 180000)
    )
    {
        lastDisconnectReason = "AUTO_TRANSPORT_RECOVERY";
        transportRecoveryCount++;
        mqttClient.disconnect();
        wsClient.close();
        lastTransportRecoveryMillis = millis();
    }
  }
  void setup() {
    Serial.begin(115200);
    Serial.printf("Free OTA: %u\n", ESP.getFreeSketchSpace());
    Serial.printf(
    "Partition OTA capable : %s\n",
    ESP.getFreeSketchSpace() > ESP.getSketchSize()
        ? "YES"
        : "NO"
    );
    pinMode(BUTTON_PIN, INPUT_PULLUP);
    pinMode(LED_PIN, OUTPUT);
    digitalWrite(LED_PIN, HIGH); 

    WiFiManager wm;
    wm.autoConnect("CCTV-MEDIUM-WS");

    initCamera();
    
    if (!mqttClient.setBufferSize(65536)) {
        if (!mqttClient.setBufferSize(32768)) {
            ESP.restart();
        }
    }
    mqttBufferSize = mqttClient.getBufferSize();
    wsClient.setInsecure();
    mqttClient.setCallback(mqttCallback);

    wsClient.addHeader(
        "Origin",
        "https://broker.miot-its.org"
    );

    wsClient.addHeader(
        "Sec-WebSocket-Protocol",
        "mqtt"
    );
    connectToMqtt();
    sendTelemetry();
  }

  void loop() {
    loopCounter++;
    handleButton();
    autoRecoverCamera();
    wsClient.poll();

    if (!mqttClient.connected()) {
        if (WiFi.status() != WL_CONNECTED) {
            lastDisconnectReason = "WIFI_LOST";
        } else if (lastDisconnectReason == "NONE") {
            lastDisconnectReason = "MQTT_STATE_" + String(mqttClient.state());
        }
        connectToMqtt();
    }
    mqttClient.loop();

    if (otaRequested) {
        if (millis() - otaRequestTimestamp > 300000) {
            otaRequested = false;
            publishOtaStatus("OTA_FAILED", -1, "OTA request expired");
        } else {
            performOTA();
        }
    }

    if (!otaRunning && image_enabled && (millis() - last_capture_time > capture_interval_ms)) {
      last_capture_time = millis();
      sendPhoto();
    }

    if (!otaRunning && telemetry_enabled && (millis() - last_telemetry_time >= telemetry_interval_ms)) {
      last_telemetry_time = millis();
      sendTelemetry();
    }
    static wl_status_t prevWifi = WL_CONNECTED;
    static unsigned long wifiDisconnectTime = 0;

    if (WiFi.status() != WL_CONNECTED)
    {
        if (prevWifi == WL_CONNECTED)
        {
            wifiDropCount++;
            wifiDisconnectTime = millis();
        }
        else
        {
            unsigned long offlineTime = millis() - wifiDisconnectTime;
            if (offlineTime > 600000)
            {
                ESP.restart();
            }
            else if (offlineTime > 300000)
            {
                static unsigned long lastReconnectAttempt = 0;
                if (millis() - lastReconnectAttempt > 60000)
                {
                    WiFi.reconnect();
                    lastReconnectAttempt = millis();
                }
            }
        }
    }
    else
    {
        wifiDisconnectTime = 0;
    }

    prevWifi = (wl_status_t)WiFi.status();
  }

  void sendPhoto() {
    if (!image_enabled) return;
    unsigned long captureStart = millis();
    camera_fb_t *fb = esp_camera_fb_get();

    if (!fb)
    {
        fbNullCount++;
        captureFailCount++;

        return;
    }

    captureOkCount++;
    frameSize = fb->len;
    if (frameSize > maxFrameSize) {
        maxFrameSize = frameSize;
    }

    String base64Data =
        base64::encode(
            fb->buf,
            fb->len
        );

    size_t dataSize =
        base64Data.length();
    base64Size = dataSize;

    if (base64Size > maxBase64Size)
    {
        maxBase64Size = base64Size;
    }
    bool success = false;

    size_t mqttLimit =
        mqttClient.getBufferSize() - 1024;

    if (dataSize >= mqttLimit)
    {
        publishFailCount++;

        esp_camera_fb_return(fb);
        return;
    } else {

        wsClient.poll();
        if (!mqttClient.connected()) {
            publishFailCount++;
            esp_camera_fb_return(fb);
            return;
        }

        unsigned long pubStart = millis();
        lastPublishAttemptMillis = millis();
        success = mqttClient.publish(
            topic_image.c_str(),
            base64Data.c_str()
        );
        publishMs = millis() - pubStart;
        if (publishMs > maxPublishMs) {
            maxPublishMs = publishMs;
        }

        if (publishMs > capture_interval_ms) {
            publishOverIntervalCount++;
        }

        static uint32_t consecutivePublishStall = 0;
        if (publishMs > 10000) {
            publishStallCount++;
            consecutivePublishStall++;
            if (consecutivePublishStall >= 3) {
                lastDisconnectReason = "PUBLISH_STALL";
                mqttClient.disconnect();
                wsClient.close();
                consecutivePublishStall = 0;
            }
        } else if (success) {
            consecutivePublishStall = 0;
        }

        if (success)
        {
            publishOkCount++;

            lastImageSuccessMillis = millis();
            unsigned long captureTime = millis() - captureStart;

            lastCaptureMs = captureTime;
            if (captureTime > maxCaptureMs)
            {
                maxCaptureMs = captureTime;
            }
        }
        else
        {
            publishFailCount++;
        }
    }

    esp_camera_fb_return(fb);
  }