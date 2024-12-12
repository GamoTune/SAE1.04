const fs = require('fs');
const csv = require('csv-parser');
const { writeToPath } = require('fast-csv');

const inputFile = 'vgsales.csv'; // Ton fichier CSV source
const outputFile = 'new_vgsales.csv'; // Le fichier de sortie

const valeursNonAcceptees = ['N/A', 'Undefined', '']; // Valeurs à nettoyer

// Fonction pour nettoyer une valeur
function nettoyerValeur(valeur) {
    if (valeursNonAcceptees.includes(valeur)) {
        return null; // Remplace par `null` (équivalent SQL : NULL)
    }
    if (typeof valeur === 'string') {
        return valeur.replace(/'/g, "''"); // Échappe les apostrophes
    }
    return valeur;
}

// Lire et nettoyer le CSV
const dataNettoyee = [];

fs.createReadStream(inputFile)
    .pipe(csv())
    .on('data', (row) => {
        const ligneNettoyee = {};
        for (const [colonne, valeur] of Object.entries(row)) {
            ligneNettoyee[colonne] = nettoyerValeur(valeur);
        }
        dataNettoyee.push(ligneNettoyee);
    })
    .on('end', () => {
        console.log('Nettoyage terminé, écriture dans le fichier...');

        // Écrire dans un nouveau fichier CSV
        writeToPath(outputFile, dataNettoyee, { headers: true })
            .on('finish', () => {
                console.log(`Fichier nettoyé écrit sous : ${outputFile}`);
            });
    })
    .on('error', (error) => {
        console.error('Erreur lors de la lecture du fichier :', error);
    });