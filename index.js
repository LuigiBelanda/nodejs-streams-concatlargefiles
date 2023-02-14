/**
 *
 *  Link e outros:
 *  https://v8.dev/features/top-level-await
 *  https://www.kaggle.com/datasets/stackoverflow/so-survey-2017?select=survey_results_public.csv
 *  https://www.kaggle.com/datasets/stackoverflow/stack-overflow-2018-developer-survey?select=survey_results_public.csv
 *  https://www.w3schools.com/nodejs/met_path_dirname.asp
 *  https://developer.mozilla.org/pt-BR/docs/Web/JavaScript/Reference/Operators/import.meta
 *  https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/filter
 *
 */

// console.log(await Promise.resolve(true));

import { dirname } from "node:path";
import { promisify } from "node:util";

import { promises } from "node:fs";
const { readdir } = promises;

import debug from "debug";
const log = debug("app:concat");

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
