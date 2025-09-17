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

// Helper function to check if date is within last 7 days (for testing)
function isWithinLast7Days(dateString) {
  if (!dateString) return false;
  
  try {
    const parsedDate = new Date(dateString.trim());
    
    if (isNaN(parsedDate.getTime())) {
      return false;
    }
    
    // Calculate 7 days ago from today (for testing)
    const today = new Date();
    const daysAgo = new Date(today.getFullYear(), today.getMonth(), today.getDate() - 7);
    
    const dateOnly = new Date(parsedDate.getFullYear(), parsedDate.getMonth(), parsedDate.getDate());
    
    return dateOnly >= daysAgo;
    
  } catch (error) {
    return false;
  }
}

// Health check endpoint
app.get('/', (req, res) => {
  res.json({ message: 'CSV Processor API is running!' });
});

// Main CSV processing endpoint
app.post('/process-csv', upload.single('csvfile'), (req, res) => {
  console.log('Starting CSV processing...');
  
  let csvFilePath;
  
  if (req.file) {
    csvFilePath = req.file.path;
  } else if (req.body.filePath) {
    csvFilePath = req.body.filePath;
  } else {
    return res.status(400).json({ 
      error: 'No file provided. Either upload a file or provide filePath in request body.' 
    });
  }
  
  const results = [];
  const batchSize = 1000;
  let currentBatch = [];
  let totalRows = 0;
  let filteredRows = 0;
  let skippedRows = 0;
  
  if (!fs.existsSync(csvFilePath)) {
    return res.status(400).json({ 
      error: 'CSV file not found', 
      path: csvFilePath 
    });
  }
  
  fs.createReadStream(csvFilePath)
    .pipe(csv({ separator: '#' }))  // Use # as delimiter instead of comma
    .on('data', (row) => {
      totalRows++;
      
      // Debug: Find the date column
      if (totalRows === 1) {
        const columns = Object.keys(row);
        console.log('All columns:', columns);
        
        // Look for columns containing "data" and "decyzji"
        const dateColumns = columns.filter(col => 
          col.toLowerCase().includes('data') || 
          col.toLowerCase().includes('decyzji') ||
          col.toLowerCase().includes('wydania')
        );
        console.log('Potential date columns:', dateColumns);
        
        // Show values for these potential date columns
        dateColumns.forEach(col => {
          console.log(`${col}:`, row[col]);
        });
      }
      
      const dateValue = row.data_wydania_decyzji;
      
      if (isWithinLast7Days(dateValue)) {
        filteredRows++;
        currentBatch.push({
          data_wydania_decyzji: dateValue,
          ...row
        });
        
        if (currentBatch.length >= batchSize) {
          results.push([...currentBatch]);
          currentBatch = [];
        }
      } else {
        skippedRows++;
      }
    })
    .on('error', (error) => {
      console.error('Error reading CSV:', error);
      res.status(500).json({ error: 'Failed to process CSV file' });
    })
    .on('end', () => {
      if (currentBatch.length > 0) {
        results.push(currentBatch);
      }
      
      console.log(`Total rows: ${totalRows}, Filtered: ${filteredRows}, Skipped: ${skippedRows}`);
      
      if (req.file) {
        fs.unlink(csvFilePath, (err) => {
          if (err) console.error('Error cleaning up file:', err);
        });
      }
      
      res.json({ 
        success: true,
        totalRowsProcessed: totalRows,
        filteredRows: filteredRows,
        skippedRows: skippedRows,
        totalBatches: results.length,
        filterCriteria: 'data_wydania_decyzji within last 7 days',
        data: results 
      });
    });
});

// Test endpoint
app.post('/test-date', (req, res) => {
  const { dateString } = req.body;
  const isValid = isWithinLast7Days(dateString);
  res.json({
    date: dateString,
    isWithinLast7Days: isValid
  });
});

app.listen(port, () => {
  console.log(`CSV Processor API running at http://localhost:${port}`);
});
