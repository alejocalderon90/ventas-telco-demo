// server.js (con mount explícito)
require('dotenv').config();
const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

app.use('/img', express.static(path.join(__dirname, 'img'))); // <-- sirve /img/*
app.use(express.static(__dirname));
app.use(express.json());

app.get(/.*/, (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.listen(PORT, () => {
  console.log(`🌐 Front en http://localhost:${PORT}`);
  console.log(`🖼️  Static /img -> ${path.join(__dirname, 'img')}`);
});
