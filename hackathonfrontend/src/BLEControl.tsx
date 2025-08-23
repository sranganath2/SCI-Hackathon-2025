import React, { useState } from "react";

const SERVICE_UUID = "12345678-1234-1234-1234-1234567890ab"; 
const CHARACTERISTIC_UUID = "abcd1234-5678-1234-5678-abcdef123456";

const BLEControl: React.FC = () => {
  const [device, setDevice] = useState<BluetoothDevice | null>(null);
  const [characteristic, setCharacteristic] = useState<BluetoothRemoteGATTCharacteristic | null>(null);
  const [status, setStatus] = useState<string>("Disconnected");

  const connectBLE = async () => {
    try {
      setStatus("Requesting device...");
      const bleDevice = await navigator.bluetooth.requestDevice({
        filters: [{ services: [SERVICE_UUID] }],
      });

      const server = await bleDevice.gatt?.connect();
      if (!server) throw new Error("GATT server not found");

      const service = await server.getPrimaryService(SERVICE_UUID);
      const char = await service.getCharacteristic(CHARACTERISTIC_UUID);

      setDevice(bleDevice);
      setCharacteristic(char);
      setStatus(`Connected to ${bleDevice.name || "Unnamed Device"}`);

      // Auto-handle disconnects
      bleDevice.addEventListener("gattserverdisconnected", () => {
        setStatus("Disconnected");
        setCharacteristic(null);
        setDevice(null);
      });

      console.log("✅ Connected to ESP32:", bleDevice.name);
    } catch (error) {
      console.error("BLE Connection Failed:", error);
      setStatus("Connection failed");
    }
  };

  const sendCommand = async (command: string) => {
    if (!characteristic) return alert("Not connected!");
    const encoder = new TextEncoder();
    try {
      await characteristic.writeValue(encoder.encode(command));
      console.log(`📡 Sent: ${command}`);
    } catch (error) {
      console.error("Failed to send command:", error);
    }
  };

  return (
    <div style={{ padding: "20px", fontFamily: "Arial" }}>
      <h2>ESP32 BLE Controller</h2>
      <p>Status: {status}</p>
      <button onClick={connectBLE} style={{ margin: "5px" }}>🔗 Connect to ESP32</button>
      <div style={{ marginTop: "10px" }}>
        <button onClick={() => sendCommand("ON")} style={{ margin: "5px" }}>💡 LED ON</button>
        <button onClick={() => sendCommand("OFF")} style={{ margin: "5px" }}>💡 LED OFF</button>
      </div>
    </div>
  );
};

export default BLEControl;
