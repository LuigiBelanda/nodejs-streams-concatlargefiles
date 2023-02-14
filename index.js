/**
 *
 *  Link e outros:
 *  https://v8.dev/features/top-level-await
 *  https://www.kaggle.com/datasets/stackoverflow/so-survey-2017?select=survey_results_public.csv
 *  https://www.kaggle.com/datasets/stackoverflow/stack-overflow-2018-developer-survey?select=survey_results_public.csv
 *  https://www.w3schools.com/nodejs/met_path_dirname.asp
 *  https://developer.mozilla.org/pt-BR/docs/Web/JavaScript/Reference/Operators/import.meta
 *  https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/filter
 *  https://httptoolkit.com/blog/unblocking-node-with-unref/
 *  https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/JSON/parse
 *  https://www.w3schools.com/js/js_json_parse.asp
 *  https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/JSON/stringify
 *  https://www.w3schools.com/js/js_json_stringify.asp
 *  https://www.npmjs.com/package/csvtojson
 * 
 * 
 *  Comandos usados:
 *  npm start (node --harmony-top-level-await index.js)
 *  npm start (DEBUG=app* --harmony-top-level-await index.js)
 *  npm start | tee log.log
 * 
 *  Outros:
 *  console.time("concat-data");
 *  console.timeEnd("concat-data");
 * 
 *  Com os consoles acima conseguimos ver quanto tempo nosso código
 *  demora para rodar tudo o que pedimos, no caso se reparar no código abaixo
 *  encontrará o console.time("concat-data"); mais acima, várias linhas de código 
 *  e lá no final o console.timeEnd("concat-data"); ou seja, o tempo que levar para rodar
 *  o código que está no meio desses comandos vai ser exibido no final  
 * 
 */

// console.log(await Promise.resolve(true));

import { dirname, join } from "node:path";
import { promisify } from "node:util";
import { promises, createReadStream, createWriteStream } from "node:fs";
import { pipeline, Transform } from "node:stream";
import debug from "debug";
import csvtojson from "csvtojson";
import jsontocsv from "json-to-csv-stream";
import StreamConcat from "stream-concat";

const { readdir } = promises;
const log = debug("app:concat");
const pipelineAsync = promisify(pipeline);

/**
 * 
 *  PRIMEIRA ETAPA
 *  Aqui garantimos os caminhos para os nossos arquivos
 *  Vemos onde esta rodando nosso index
 *  E localizamos nossa pasta com os arquivos csv etc
 * 
 */

// pega o caminho da onde esta rodando o arquivo
const { pathname: currentFile } = new URL(import.meta.url);
// console.log(currentFile);

// pega qual o diretório que o arquivo esta rodando
const cwd = dirname(currentFile);
// console.log(cwd);

// mostra o caminho para a pasta onde estão os arquivos csv e outros
const filesDir = `${cwd}/dataset`;
// console.log(filesDir);

// mostra onde será a saída dos dados
const output = `${cwd}/final.csv`;
// console.log(output);

console.time("concat-data");

// lemos o diretório em que estão os arquivos com dados e pegamos só os .csv
const files = (await readdir(filesDir))
    .filter((item) => !!!~item.indexOf(".zip"));
log(`Processing: ${files}`);

/**
 * 
 *  SEGUNDA ETAPA:
 *  Aqui fazemos uma pequena barra de progresso para ter algum sinal
 *  para o user que o programa esta rodando e não está travado
 * 
 */

// barra de progresso
const ONE_SECOND = 1000;
// quando os outros processos acabarem ele morre junto (unref)
setInterval(() => process.stdout.write("."), ONE_SECOND).unref();

/**
 * 
 *  TERCEIRA ETAPA:
 *  Aqui começamos a trabalhar propriamente com as streams
 *  Lendo nossos arquivos ( createReadStream, streams, combinedStreams ) e juntando as streams
 *  dos arquivos que lemos em uma só ( StreamConcat ) 
 *  Transformando nossos dados de cvs para json ( csvtojson() )
 *  Manipulando os dados e pegando apenas os dados que queremos (Transform / handleStream)
 *  Transformando os dados de json para csv
 *  Por fim, enviando esses dados para alguma saída (createWriteStream / Writable stream)
 * 
 *  Essas etapas são definidas no nosso pipeline (pipelineAsync).
 * 
 *  Chunk = nossa dado / um pedaço do arquivo que foi lido
 *  cb = callback / o que estamos retornando (1º param: error | 2º param: nosso dado) 
 * 
 */


// lendo os arquivos (readable stream)
const streams = files.map(
    item => createReadStream(join(filesDir, item))
)

const combinedStreams = new StreamConcat(streams);

// trabalhando com os dados dos arquivos - pegando apenas os dados que queremos - mudando tipo dos dados
const handleStream = new Transform({
    transform: (chunk, enconding, cb) => {
        const data = JSON.parse(chunk);

        const output = {
            id: data.Respondent,
            country: data.Country
        }

        // log(`Id: ${output.id}`);
        return cb(null, JSON.stringify(output));
    }
})

// escrevendo os dados (writable stream)
const finalStream = createWriteStream(output);

// etapas de execução da stream / pipeline
await pipelineAsync(
    combinedStreams,
    csvtojson(),
    handleStream,
    jsontocsv(),
    finalStream
);

log(`${files.length} files merged! on ${output}`);
console.timeEnd("concat-data");