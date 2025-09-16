const express = require('express');
const csv = require('csv-parser');
const fs = require('fs');
const cors = require('cors');
const multer = require('multer');

const app = express();
const port = process.env.PORT || 3000;

// Configure multer for file uploads
const upload = multer({ dest: 'uploads/' });

// Enable CORS for n8n to access this API
app.use(cors());
app.use(express.json());

// Health check endpoint
app.get('/', (req, res) => {
  res.json({ message: 'CSV Processor API is running!' });
});

// Main CSV processing endpoint - accepts file upload
app.post('/process-csv', upload.single('csvfile'), (req, res) => {
  console.log('Starting CSV processing...');
  
  // Use uploaded file if provided, or fall back to local path
  let csvFilePath;
  
  if (req.file) {
    csvFilePath = req.file.path;
    console.log('Processing uploaded file:', req.file.originalname);
  } else if (req.body.filePath) {
    csvFilePath = req.body.filePath;
    console.log('Processing file from path:', csvFilePath);
  } else {
    return res.status(400).json({ 
      error: 'No file provided. Either upload a file or provide filePath in request body.' 
    });
  }
  
  const results = [];
  const batchSize = 1000; // Process 1000 rows at a time
  let currentBatch = [];
  let totalRows = 0;
  
  // Check if file exists
  if (!fs.existsSync(csvFilePath)) {
    return res.status(400).json({ 
      error: 'CSV file not found', 
      path: csvFilePath 
    });
  }
  
  fs.createReadStream(csvFilePath)
    .pipe(csv())
    .on('data', (row) => {
      totalRows++;
      
      // CUSTOMIZE THIS PART: Extract only the data you need
      // Replace 'column1', 'column2' with your actual column names
      const extractedData = {
        // Example: If your CSV has columns 'name', 'email', 'age'
        name: row.name,           // Change 'name' to your column name
        email: row.email,         // Change 'email' to your column name
        age: row.age              // Change 'age' to your column name
        
        // Add more fields as needed:
        // date: row.created_date,
        // status: row.status,
      };
      
      currentBatch.push(extractedData);
      
      // When we have enough rows, save this batch
      if (currentBatch.length >= batchSize) {
        results.push([...currentBatch]);
        currentBatch = [];
        console.log(`Processed ${results.length * batchSize} rows so far...`);
      }
    })
    .on('error', (error) => {
      console.error('Error reading CSV:', error);
      res.status(500).json({ error: 'Failed to process CSV file' });
    })
    .on('end', () => {
      // Don't forget the last batch if it's not full
      if (currentBatch.length > 0) {
        results.push(currentBatch);
      }
      
      console.log(`CSV processing completed! Total rows: ${totalRows}, Batches: ${results.length}`);
      
      // Clean up uploaded file
      if (req.file) {
        fs.unlink(csvFilePath, (err) => {
          if (err) console.error('Error cleaning up file:', err);
        });
      }
      
      res.json({ 
        success: true,
        totalRows: totalRows,
        totalBatches: results.length,
        data: results 
      });
    });
});

// Start the server
app.listen(port, () => {
  console.log(`CSV Processor API running at http://localhost:${port}`);
  console.log('Ready to process your CSV files!');
});
