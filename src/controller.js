const records = require("./records.model");
const csv = require("csv-parser");
const fs = require("fs");

// Helper function for deleting files
function deleteTempFile(filePath) {
  fs.unlink(filePath, (unlinkErr) => {
    if (unlinkErr) {
      console.error(`Error deleting temp file ${filePath}:`, unlinkErr);
    } else {
      console.log(`Temp file ${filePath} was deleted.`);
    }
  });
}

const upload = async (req, res) => {
  if (!req.file) {
    return res.status(400).send("No file uploaded.");
  }

  const filePath = req.file.path;
  const recordsToSave = [];
  const batchSize = 1000;
  // NUEVO: Define el número máximo de operaciones de inserción en DB concurrentes.
  // Ajusta este valor (e.g., 2, 5, 10) según la capacidad de tu MongoDB y tu servidor.
  const maxConcurrentDbOperations = 5;
  // NUEVO: Usamos un Set para almacenar las promesas de inserción que están activas.
  const activeDbOperations = new Set();

  try {
    await new Promise((resolve, reject) => {
      const readStream = fs.createReadStream(filePath);
      const csvStream = csv();

      readStream
        .pipe(csvStream)
        .on("data", async (data) => {
          // NUEVO: Si tenemos demasiadas operaciones de DB activas,
          // pausamos la lectura del stream para aplicar contrapresión.
          if (activeDbOperations.size >= maxConcurrentDbOperations) {
            readStream.pause();
          }

          recordsToSave.push({
            id: data.id,
            firstname: data.firstname,
            lastname: data.lastname,
            email: data.email,
            email2: data.email2,
            profession: data.profession,
          });

          if (recordsToSave.length >= batchSize) {
            const currentBatch = [...recordsToSave];
            recordsToSave.length = 0; // Limpia el array para el próximo lote

            // Llama a insertMany sin 'await' para que se ejecute en paralelo.
            const insertionPromise = records
              .insertMany(currentBatch)
              .then(() => {
                // Cuando una operación de inserción termina, la eliminamos del set.
                activeDbOperations.delete(insertionPromise);
                // Si el stream estaba pausado y ahora tenemos capacidad, lo reanudamos.
                if (
                  readStream.isPaused() &&
                  activeDbOperations.size < maxConcurrentDbOperations
                ) {
                  readStream.resume();
                }
              })
              .catch((err) => {
                // Si hay un error en la inserción, lo eliminamos del set.
                activeDbOperations.delete(insertionPromise);
                console.error("Error saving batch to MongoDB:", err);
                // Destruye el stream para detener el procesamiento posterior y propaga el error.
                readStream.destroy(err);
                reject(err); // Rechaza la promesa principal para salir del flujo.
              });

            // Añade la promesa de inserción al set de operaciones activas.
            activeDbOperations.add(insertionPromise);
          }
        })
        .on("end", async () => {
          // Procesa cualquier registro restante que no formó un lote completo.
          if (recordsToSave.length > 0) {
            console.log(`Saving remaining ${recordsToSave.length} records...`);
            const remainingPromise = records
              .insertMany(recordsToSave)
              .catch((err) => {
                console.error(
                  "Error saving remaining records to MongoDB:",
                  err,
                );
                // Si esta inserción falla, la capturaremos en Promise.allSettled.
                return Promise.reject(err);
              });
            activeDbOperations.add(remainingPromise);
          }

          // Espera a que todas las operaciones de base de datos activas y restantes se completen.
          // Promise.allSettled permite que algunas promesas fallen sin detener el resto.
          try {
            const results = await Promise.allSettled(
              Array.from(activeDbOperations),
            );
            const failedResults = results.filter(
              (result) => result.status === "rejected",
            );

            if (failedResults.length > 0) {
              // Si hay alguna promesa rechazada, rechazamos la promesa principal.
              reject(failedResults[0].reason); // Propagamos el primer error encontrado.
            } else {
              resolve(); // Resuelve la promesa principal si todas las operaciones fueron exitosas.
            }
          } catch (err) {
            // Este catch es para errores inesperados en el Promise.allSettled.
            reject(err);
          }
        })
        .on("error", (err) => {
          // Captura errores del stream de archivos (fs.createReadStream) o del parseo CSV.
          console.error("Error processing CSV stream:", err);
          reject(err);
        });
    });

    // Si llegamos aquí, todo el procesamiento fue exitoso.
    res
      .status(200)
      .send("File processed and data saved to database successfully.");
  } catch (error) {
    // Este bloque catch principal maneja todas las promesas rechazadas
    // (errores de inserción, errores de stream, o cualquier excepción no controlada).
    console.error("Error during file upload/processing:", error);
    res.status(500).send("Error processing file.");
  } finally {
    // Siempre intenta eliminar el archivo temporal, independientemente del éxito o fracaso.
    deleteTempFile(filePath);
  }
};

const list = async (_, res) => {
  try {
    const data = await records.find({}).sort({ _id: -1 }).limit(10).lean();
    return res.status(200).json(data);
  } catch (err) {
    console.error("Error listing records:", err);
    return res.status(500).send("Error retrieving records.");
  }
};

module.exports = {
  upload,
  list,
};
