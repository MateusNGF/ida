import { readdir } from "fs/promises" 
import { dirname, join } from "path"
import { fileURLToPath } from "url"

import debug from 'debug'
import { WriteStream, createReadStream, createWriteStream } from "fs"
import StreamConcat from "stream-concat"
import { pipeline } from "stream/promises"
import { Readable, Transform } from "stream"


import ExcelJS from 'exceljs'
import { createObjectCsvWriter } from "csv-writer"


const applicationLogging = debug('app:concat')

const currentFile = fileURLToPath(import.meta.url)
const __dirname = dirname(currentFile)

const pathContentFiles = join(__dirname, 'dados/2_1_Populacao_residente_Area_territorial_Densidade_demografica_xlsx');
const pathOutputContentFiles = join(__dirname, 'dados/processed');

const TAG_TIME_PROCESSING = "TIME_PROCESSING_CONCAT_FILES"	
console.time(TAG_TIME_PROCESSING)

const files = (await readdir(pathContentFiles)).filter(file => file.indexOf(".xlsx")).sort((a, b) => a.localeCompare(b))

setInterval(() => {
    console.timeLog(TAG_TIME_PROCESSING)
}, 1000).unref()


const header = [
    { id: 0, title: "Unidade da Federação e Município" },
    { id: 1, title: "População residente (Pessoas)" },
    { id: 2, title: "Área da unidade territorial (Quilômetros quadrados)" },
    { id: 3, title: "Densidade demográfica (Habitante por quilômetro quadrado)" },
]

const csvWriter = createObjectCsvWriter({
    path: join(pathOutputContentFiles, '2_1_Populacao_residente_Area_territorial_Densidade_demografica.csv'), // Nome do arquivo CSV
    header: header
});

const streams = files.map( item => item.indexOf("MG") && createReadStream(join(pathContentFiles, item)))
const streamsConcateded = new StreamConcat(streams)

const processExcelFiles = new Transform({
    async transform(chunk, encoding, cb) {
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.load(chunk);
        const planilha = workbook.worksheets[0];
        const totalDeLinhas = planilha.actualRowCount
        const result = []
        planilha.eachRow(async (row, rowNumber) => {
            if (rowNumber <= 5 || rowNumber >= totalDeLinhas) return
            result.push(row.values.filter(Boolean))
            
        });
        await csvWriter.writeRecords(result);
        cb(null)
    }
});



await pipeline(
    streamsConcateded,
    processExcelFiles
)


console.timeEnd(TAG_TIME_PROCESSING)