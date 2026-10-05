import express from 'express';
import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import mongoose from 'mongoose';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 10000;
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/turbowarp_cloud';

// -----------------------------------------
// MongoDB Schema & Model
// -----------------------------------------
const variableSchema = new mongoose.Schema({
  projectId: { type: String, required: true },
  name: { type: String, required: true },
  value: { type: mongoose.Schema.Types.Mixed, required: true },
}, { timestamps: true });

// Ensure unique variables per project
variableSchema.index({ projectId: 1, name: 1 }, { unique: true });
const Variable = mongoose.model('Variable', variableSchema);

// Connect to MongoDB
mongoose.connect(MONGO_URI)
  .then(() => console.log('📁 Connected to MongoDB successfully.'))
  .catch(err => console.error('❌ MongoDB connection error:', err));

// -----------------------------------------
// WebSocket Server Logic
// -----------------------------------------
const rooms = new Map();

wss.on('connection', (ws) => {
  let currentProjectId = null;

  ws.on('message', async (message) => {
    try {
      const data = JSON.parse(message.toString());

      switch (data.method) {
        case 'handshake':
          currentProjectId = data.project_id;
          
          if (!rooms.has(currentProjectId)) {
            rooms.set(currentProjectId, new Set());
          }
          rooms.get(currentProjectId).add(ws);

          // Fetch stored variables from MongoDB and send to client
          const variables = await Variable.find({ projectId: currentProjectId });
          for (const variable of variables) {
            ws.send(JSON.stringify({
              method: 'set',
              name: variable.name,
              value: variable.value
            }) + '\n');
          }
          break;

        case 'set':
          if (!currentProjectId) return;

          // Upsert the variable value in MongoDB
          await Variable.findOneAndUpdate(
            { projectId: currentProjectId, name: data.name },
            { value: data.value },
            { upsert: true, new: true }
          );

          // Broadcast to all other clients in the same project room
          const clients = rooms.get(currentProjectId);
          if (clients) {
            const broadcastMessage = JSON.stringify({
              method: 'set',
              name: data.name,
              value: data.value
            }) + '\n';

            clients.forEach((client) => {
              if (client !== ws && client.readyState === WebSocket.OPEN) {
                client.send(broadcastMessage);
              }
            });
          }
          break;

        default:
          console.log(`Unknown method: ${data.method}`);
      }
    } catch (err) {
      console.error('Error handling message:', err);
    }
  });

  ws.on('close', () => {
    if (currentProjectId && rooms.has(currentProjectId)) {
      const clients = rooms.get(currentProjectId);
      clients.delete(ws);
      if (clients.size === 0) {
        rooms.delete(currentProjectId);
      }
    }
  });
});

// Basic HTTP health check for Render
app.get('/', (req, res) => {
  res.send('TurboWarp Cloud Variable Server is running.');
});

// Listen on 0.0.0.0 for Render routing compatibility
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Server listening on port ${PORT}`);
});
