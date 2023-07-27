const AWS = require("aws-sdk");
const bunyan = require("bunyan");
const fs = require("fs");
require("dotenv").config();

AWS.config.update({
  accessKeyId: process.env.ACCESS_KEY,
  secretAccessKey: process.env.SECRET_KEY,
});

const s3 = new AWS.S3();
const bucketName = process.env.S3_BUCKET;
const maxKeys = 1000; // Adjust as needed
const markersFile = "./markers.json";

// Initialize Bunyan logger
const log = bunyan.createLogger({ name: "myapp" });

async function deleteVersionedObjectsAndMarkers() {
  let listParams = { Bucket: bucketName, MaxKeys: maxKeys };

  // Check if markers file exists and load it
  if (fs.existsSync(markersFile)) {
    const markers = JSON.parse(fs.readFileSync(markersFile, "utf8"));
    listParams.KeyMarker = markers.NextKeyMarker;
    listParams.VersionIdMarker = markers.NextVersionIdMarker;
  }

  let response;
  let counter = 0; // Counter to track the number of deleted objects and markers

  do {
    try {
      response = await s3.listObjectVersions(listParams).promise();
    } catch (error) {
      log.error("Error listing object versions:", error);
      continue;
    }

    const deletePromises = response.DeleteMarkers.map((marker) => {
      const deleteParams = {
        Bucket: bucketName,
        Delete: {
          Objects: [{ Key: marker.Key, VersionId: marker.VersionId }],
          Quiet: false,
        },
      };

      return s3
        .deleteObjects(deleteParams)
        .promise()
        .then(() => {
          counter += 1;
          log.info(
            `Deleted object: ${marker.Key}, VersionId: ${marker.VersionId}. Total objects deleted so far: ${counter}`
          );
        })
        .catch((error) => log.error("Error deleting object:", error));
    });

    await Promise.all(deletePromises);

    log.info("Batch completed. Total objects and markers deleted:", counter);

    listParams.KeyMarker = response.NextKeyMarker;
    listParams.VersionIdMarker = response.NextVersionIdMarker;

    // Save the markers after each batch
    fs.writeFileSync(
      markersFile,
      JSON.stringify({
        NextKeyMarker: response.NextKeyMarker,
        NextVersionIdMarker: response.NextVersionIdMarker,
      }),
      "utf8"
    );
  } while (response.IsTruncated);
}

// Call the function to initiate the deletion
deleteVersionedObjectsAndMarkers();
