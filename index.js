// Assuming you have already set up the AWS SDK and obtained valid credentials
const AWS = require("aws-sdk");
require('dotenv').config();

AWS.config.update({
  accessKeyId: process.env.ACCESS_KEY,
  secretAccessKey: process.env.SECRET_KEY,
});

const s3 = new AWS.S3();

const bucketName = process.env.S3_BUCKET;

async function deleteVersionedObjectsAndMarkers() {
  try {
    // List all object versions (including delete markers) in the bucket
    const listParams = {
      Bucket: bucketName,
      MaxKeys: 5, // Adjust the value based on your bucket's object count
    };
    let response;
    let counter = 0; // Counter to track the number of deleted objects and markers

    do {
      response = await s3.listObjectVersions(listParams).promise();

      // Delete objects and delete markers
      const deleteParams = {
        Bucket: bucketName,
        Delete: {
          Objects: [],
          Quiet: false,
        },
      };

      response.DeleteMarkers.forEach((marker) => {
        deleteParams.Delete.Objects.push({
          Key: marker.Key,
          VersionId: marker.VersionId,
        });
      });

      // Perform the actual delete operation
      if (deleteParams.Delete.Objects.length > 0) {
        await s3.deleteObjects(deleteParams).promise();
        counter += 1; // Update the counter with the number of deleted objects and markers
        console.log("Total objects and markers deleted:", counter);
      }

      // Check if there are more objects to delete
      listParams.KeyMarker = response.NextKeyMarker;
      listParams.VersionIdMarker = response.NextVersionIdMarker;
    } while (response.IsTruncated);
  } catch (error) {
    console.error("Error deleting versioned objects and markers:", error);
  }
}

// Call the function to initiate the deletion
deleteVersionedObjectsAndMarkers();
