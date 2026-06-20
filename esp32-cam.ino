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

  #define LED_PIN 4
  #define DEVICE_HOSTNAME "cctv-medium-local"
  #define BUTTON_PIN 13

  unsigned long buttonPressStart = 0;
  const char* mqtt_broker   = "xxx"; 
  const int   mqtt_port     = 443;           
  const char* mqtt_path     = "/mqtt";
  const char* mqtt_user     = "xxx";      
  const char* mqtt_pass     = "xxx";   
  const char* device_id     = "xxx"; 

  const String topic_image     = "ws/camera/" + String(device_id) + "/image";
  const String topic_telemetry = "ws/camera/" + String(device_id) + "/telemetry";
  const String topic_status    = "ws/camera/" + String(device_id) + "/status";

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
  const unsigned long capture_interval = 3000; 

  unsigned long last_telemetry_time = 0;
  const unsigned long telemetry_interval = 60000;
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

  #if ARDUINOJSON_VERSION_MAJOR >= 7
    JsonDocument doc;
  #else
    StaticJsonDocument<512> doc;
  #endif

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

  void initCamera() {
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

    config.frame_size = FRAMESIZE_SVGA;  
    config.jpeg_quality = 15;            
    config.fb_count = 2;

    esp_err_t err = esp_camera_init(&config);
    if (err != ESP_OK) { ESP.restart(); }
    
    Serial.println("Kamera diinisialisasi pada resolusi SVGA (Medium)");
  }
  void resetCameraOnly()
  {
      cameraResetCount++;
      lastRecoveryReason = "BUTTON";

      Serial.println(
        "[BUTTON] CAMERA RESET"
      );

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
              Serial.println(
                  "[BUTTON] ESP RESTART"
              );
              buttonPressStart = 0;
              ESP.restart();
          }
          else if (held >= 5000)
          {
              Serial.println(
                  "[BUTTON] WIFI RESET"
              );

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

          wsWrapper.pushData(
            (const uint8_t*)msg.data().c_str(),
            msg.length()
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
    pinMode(BUTTON_PIN, INPUT_PULLUP);
    pinMode(LED_PIN, OUTPUT);
    digitalWrite(LED_PIN, HIGH); 

    WiFiManager wm;
    wm.autoConnect("CCTV-MEDIUM-WS");

    initCamera();
    
    if (!mqttClient.setBufferSize(65536)) {
        Serial.println("MQTT 64KB failed");

        if (!mqttClient.setBufferSize(32768)) {
            Serial.println("MQTT 32KB failed");
            ESP.restart();
        }
    }
    Serial.printf(
        "MQTT BUFFER=%u\n",
        mqttClient.getBufferSize()
    );
    wsClient.setInsecure();

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

    if (millis() - last_capture_time > capture_interval) {
      last_capture_time = millis();
      sendPhoto();
    }

    if (millis() - last_telemetry_time >= telemetry_interval) {
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

        if (publishMs > capture_interval) {
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
        } else if (success) {1
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