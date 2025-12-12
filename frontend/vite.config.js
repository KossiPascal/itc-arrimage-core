
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import * as dotenv from 'dotenv';
import path from 'path';

// Charger le .env global
dotenv.config({ path: path.resolve(__dirname, '../../.env'), override: true });

export default defineConfig({
  plugins: [react()],
  optimizeDeps: {
    include: ["@monaco-editor/react", "monaco-editor"]
  },
  build: {
    commonjsOptions: {
      include: [/monaco-editor/, /node_modules/],
    },
    rollupOptions: {
      output: {
        manualChunks: {
          monaco: ["monaco-editor"],
        }
      }
    }
  },
  define: {
    'import.meta.env.VITE_APP_NAME': JSON.stringify(process.env.APP_NAME || "DHIS2 Sync App"),
    'import.meta.env.VITE_APP_SUBNAME': JSON.stringify(process.env.APP_SUBNAME || "ITC App"),
    'import.meta.env.VITE_APP_VERSION': JSON.stringify(process.env.APP_VERSION || "1"),
    
    'import.meta.env.VITE_API_URL': JSON.stringify(process.env.API_URL),
    'import.meta.env.VITE_TIMEOUT': JSON.stringify(process.env.TIMEOUT || '60'),
  },
  server: {
    port: 5173,
    // proxy: {
    //   '/api': 'http://localhost:5000'
    // }
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'src'),
    },
  },
})
