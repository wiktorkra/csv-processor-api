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

// Helper function to check if date is within last 365 days (for testing)
function isWithinLast7Days(dateString) {
  if (!dateString) return false;
  
  try {
    // Your date format is: M/D/YYYY H:MM AM/PM (e.g., "7/14/2016 12:00 AM")
    // JavaScript's Date constructor can parse this format directly
    const parsedDate = new Date(dateString.trim());
    
    // Check if date is valid
    if (isNaN(parsedDate.getTime())) {
      console.log(`Invalid date format: ${dateString}`);
      return false;
    }
    
    // Calculate 365 days ago from today (temporarily for testing)
    const today = new Date();
    const sevenDaysAgo = new Date(today.getFullYear(), today.getMonth(), today.getDate() - 365);
    
    // Get just the date part (ignore time) for comparison
    const dateOnly = new Date(parsedDate.getFullYear(), parsedDate.getMonth(), parsedDate.getDate());
    
    // Check if the date is within the last 365 days (inclusive)
    const result = dateOnly >= sevenDaysAgo;
    
    // Log for debugging (remove this in production)
    if (result) {
      console.log(`✓ Date ${dateString} is within last 365 days`);
    }
    
    return result;
    
  } catch (error) {
    console.log(`Error parsing date: ${dateString}`, error);
    return false;
  }
}

// Health check endpoint
app.get('/', (req, res) => {
  res.json({ message: 'CSV Processor API is running!' });
});

// Main CSV processing endpoint - accepts file upload with date filtering
app.post('/process-csv', upload.single('csvfile'), (req, res) => {
  console.log('Starting CSV processing with date filtering...');
  
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
  let filteredRows = 0;
  let skippedRows = 0;
  
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
      
      // Check if data_wydania_decyzji is within last 365 days
      const dateValue = row.data_wydania_decyzji;
      
      // Log first 10 date values for debugging
      if (totalRows <= 10) {
        console.log(`Row ${totalRows} - Date value: "${dateValue}" (type: ${typeof dateValue})`);
      }
      
      if (isWithinLast7Days(dateValue)) {
        filteredRows++;
        
        // CUSTOMIZE THIS PART: Extract only the data you need
        // Add all the columns you want to return
        const extractedData = {
          // Keep the date column
          data_wydania_decyzji: dateValue,
          
          // Include ALL columns if needed:
          ...row  // This includes all columns from the CSV
        };
        
        currentBatch.push(extractedData);
        
        // When we have enough filtered rows, save this batch
        if (currentBatch.length >= batchSize) {
          results.push([...currentBatch]);
          currentBatch = [];
          console.log(`Processed ${results.length * batchSize} filtered rows so far...`);
        }
      } else {
        skippedRows++;
      }
      
      // Log progress every 10000 rows
      if (totalRows % 10000 === 0) {
        console.log(`Progress: ${totalRows} total rows processed, ${filteredRows} matched filter, ${skippedRows} skipped`);
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
      
      console.log(`CSV processing completed!`);
      console.log(`Total rows processed: ${totalRows}`);
      console.log(`Rows matching filter (last 365 days): ${filteredRows}`);
      console.log(`Rows skipped: ${skippedRows}`);
      console.log(`Batches created: ${results.length}`);
      
      // Clean up uploaded file
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
        filterCriteria: 'data_wydania_decyzji within last 365 days (testing)',
        data: results 
      });
    });
});

// Optional: Endpoint to test date filtering logic
app.post('/test-date', (req, res) => {
  const { dateString } = req.body;
  const isValid = isWithinLast7Days(dateString);
  res.json({
    date: dateString,
    isWithinLast365Days: isValid,
    365DaysAgo: new Date(Date.now() - 365 * 24 * 60 * 60 * 1000).toISOString()
  });
});

// Start the server
app.listen(port, () => {
  console.log(`CSV Processor API running at http://localhost:${port}`);
  console.log('Ready to process your CSV files with date filtering!');
});
