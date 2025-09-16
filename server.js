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
    .pipe(csv({ separator: '#' }))  // Use # as delimiter instead of comma
    .on('data', (row) => {
      totalRows++;
      
      // Extract your Polish building permit data columns
      // Handle missing columns with || '' to avoid undefined values
      const extractedData = {
        numer_urzad: row.numer_urzad || '',
        nazwa_organu: row.nazwa_organu || '',
        adres_organu: row.adres_organu || '',
        data_wplywu_wniosku: row.data_wplywu_wniosku || '',
        numer_decyzji_urzedu: row.numer_decyzji_urzedu || '',
        data_wydania_decyzji: row.data_wydania_decyzji || '',
        nazwa_inwestor: row.nazwa_inwestor || '',
        wojewodztwo: row.wojewodztwo || '',
        miasto: row.miasto || '',
        terc: row.terc || '',
        cecha: row.cecha || '',
        cecha_2: row['cecha (2)'] || '',  // Handle column with spaces/parentheses
        ulica: row.ulica || '',
        ulica_dalej: row.ulica_dalej || '',
        nr_domu: row.nr_domu || '',
        rodzaj_inwestycji: row.rodzaj_inwestycji || '',
        kategoria: row.kategoria || '',
        nazwa_zamierzenia_bud: row.nazwa_zamierzenia_bud || '',
        nazwa_zam_budowalnego: row.nazwa_zam_budowalnego || '',
        kubatura: row.kubatura || '',
        projektant_nazwisko: row.projektant_nazwisko || '',
        projektant_imie: row.projektant_imie || '',
        projektant_numer_uprawnien: row.projektant_numer_uprawnien || '',
        jednostka_numer_ew: row.jednostka_numer_ew || '',
        obreb_numer: row.obreb_numer || '',
        numer_dzialki: row.numer_dzialki || '',
        numer_arkusza_dzialki: row.numer_arkusza_dzialki || '',
        jednostka_stara_numeracja: row.jednostka_stara_numeracja_z_wniosku || '',
        stara_numeracja_obreb: row.stara_numeracja_obreb_z_wnioskiu || '',
        stara_numeracja_dzialka: row.stara_numeracja_dzialka_z_wniosku || ''
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
