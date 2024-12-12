const fs = require('fs');
const csv = require('csv-parser');

// Configuration
const inputFile = 'new_vgsales.csv'; // Fichier CSV nettoyé
const outputFile = 'vgsales.sql'; // Fichier de sortie SQL
const tableName = 'jeux'; // Nom de la table SQL

// Fonction pour échapper les valeurs
function escapeSQL(value) {
    if (value === null || value === undefined || value === '') {
        return 'NULL';
    }
    if (typeof value === 'string') {
        return `'${value.replace(/'/g, "''")}'`; // Échappe les apostrophes
    }
    return value; // Les nombres restent tels quels
}

// Lire le fichier CSV et générer les instructions SQL
const sqlStatements = [];
fs.createReadStream(inputFile)
    .pipe(csv())
    .on('data', (row) => {
        const columns = [
            escapeSQL(row['rank_id']),
            escapeSQL(row['name']),
            escapeSQL(row['platform']),
            escapeSQL(row['release_year']),
            escapeSQL(row['genre']),
            escapeSQL(row['publisher']),
            escapeSQL(row['na_sales']),
            escapeSQL(row['eu_sales']),
            escapeSQL(row['jp_sales']),
            escapeSQL(row['other_sales']),
            escapeSQL(row['global_sales']),
        ];

        // Créer une instruction INSERT pour chaque ligne
        const sql = `INSERT INTO \`${tableName}\` (\`rank_id\`, \`name\`, \`platform\`, \`release_year\`, \`genre\`, \`publisher\`, \`na_sales\`, \`eu_sales\`, \`jp_sales\`, \`other_sales\`, \`global_sales\`) VALUES (${columns.join(', ')});`;
        sqlStatements.push(sql);
    })
    .on('end', () => {
        // Écrire les instructions SQL dans un fichier
        fs.writeFileSync(outputFile, sqlStatements.join('\n'), 'utf-8');
        console.log(`Instructions SQL générées dans le fichier : ${outputFile}`);
    })
    .on('error', (error) => {
        console.error('Erreur lors de la lecture du fichier :', error);
    });