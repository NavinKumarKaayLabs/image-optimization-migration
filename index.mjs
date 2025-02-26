import fs from "fs";
import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import Sharp from "sharp";
import dotenv from "dotenv";
import {
  Worker,
  isMainThread,
  parentPort,
  workerData,
  threadId,
} from "worker_threads";
import os from "os";
import { promisify } from "util";

import Redis from 'ioredis';
const redis = new Redis()

dotenv.config();
const readFileAsync = promisify(fs.readFile);
const writeFileAsync = promisify(fs.writeFile);

const NUM_CORES = os.cpus().length -  1; // Get number of CPU cores
const BATCH_SIZE = 3; // Number of images to process in parallel per worker
const RETRY_ATTEMPTS = 3; // Number of retry attempts for failed operations

const s3Client = new S3Client({
  credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  },
  region: process.env.AWS_DEFAULT_REGION || "ap-south-1",
  //   maxAttempts: 3, // Built-in retry mechanism
});
const S3_ORIGINAL_IMAGE_BUCKET = process.env.originalImageBucketName;
const S3_TRANSFORMED_IMAGE_BUCKET = process.env.transformedImageBucketName;
const TRANSFORMED_IMAGE_CACHE_TTL = process.env.transformedImageCacheTTL;
const MAX_IMAGE_SIZE = parseInt(process.env.maxImageSize);

const imageSizes = [
  320,
  420,
  768,
  1024,
  1200,
  1600,
  1920,
  2560,
  3840,
  "original",
];
  

const updateObjectInArray = async (id, processed) => {
  const data = await redis.get("imageArray"); // Get array from Redis

  if (!data) {
      console.log("No data found!");
      return;
  }

  let array = JSON.parse(data); // Convert string back to array

  // Find and update the object
  array = array.map(item => {
      const returnItem = item;
      if(returnItem.id === id){
          returnItem.image_processed = processed;
      }
      // (item.id === id ? { ...item, ...newData } : item)
      return item;
  });

  await redis.set("imageArray", JSON.stringify(array)); // Save updated array back to Redis
  console.log("Array updated in Redis", id , processed);
};

// Worker thread code
if (!isMainThread) {
  const { images, sizes } = workerData;

  const processImage = async (image, imageIndex) => {
    const results = [];
    for (const size of sizes) {
      try {
        const appendQuery =
          size === "original" ? "original" : `format=avif,width=${size}`;
        await handler(`${image.product_image}?${appendQuery}`);
        results.push({ success: true, image: image.id, size });
      } catch (error) {
        console.log('handler error-->', error)
        results.push({
          success: false,
          image: image.id,
          size,
          error: error?.message,
        });
      }
    }
    if (results.every((result) => result.success)) {
       await updateObjectInArray(image.id, true);
    } else {
       await updateObjectInArray(image.id, true);
    }

    return results;
  };

  const processBatch = async () => {
    try {
      const results = [];
      for (const [imageIndex, image] of images.entries()) {
        const result = await processImage(image, imageIndex); // Process one image at a time
        results.push(...result);
      }
      parentPort.postMessage(results.flat());
    } catch (error) {
      console.log('error in process batch', error)
      parentPort.postMessage({ error: error?.message });
    }
  };

  await processBatch();
}

// Main thread code
async function processImagesInParallel(imagesToProcess) {
  const workers = [];
  const results = [];

  // Split images into batches for each worker
  const imagesPerWorker = Math.ceil(imagesToProcess.length / NUM_CORES);
  console.log("imagesPerWorker", imagesPerWorker, NUM_CORES);

  for (let i = 0; i < NUM_CORES; i++) {
    const workerImages = imagesToProcess.slice(
      i * imagesPerWorker,
      (i + 1) * imagesPerWorker
    );

    if (workerImages.length > 0) {
      const worker = new Worker(new URL(import.meta.url), {
        workerData: {
          images: workerImages,
          sizes: imageSizes,
        },
      });

      workers.push(
        new Promise((resolve, reject) => {
          worker.on("message", (result) => {
            results.push(...result);
            resolve();
          });
          worker.on("error", reject);
          worker.on("exit", (code) => {
            console.log("worker exist")
            if (code !== 0)
              reject(new Error(`Worker stopped with exit code ${code}`));
          });
        })
      );
    }
  }

  await Promise.all(workers);
  return results;
}

async function handler(event) {
  const imagePathArray = decodeURIComponent(new URL(event).pathname).split("/");
  const searchPath = new URL(event).search;
  const operationsPrefix = searchPath
    ? searchPath.substring(1, searchPath.length)
    : "original";
  imagePathArray.shift();
  const originalImagePath = imagePathArray.join("/");
  let startTime = performance.now();
  let originalImageBody;
  let contentType;
  const imageExist = async () => {
    try {
      const getOriginalImageCommand = new GetObjectCommand({
        Bucket: S3_TRANSFORMED_IMAGE_BUCKET,
        Key: originalImagePath + "/" + operationsPrefix,
      });
      const getOriginalImageCommandOutput = await s3Client.send(
        getOriginalImageCommand
      );
      contentType = getOriginalImageCommandOutput.ContentType;
      return true;
    } catch (error) {
      return false;
    }
  };
  const abc = await imageExist();
  if (abc) {
    console.log("image already exist",originalImagePath + "/" + operationsPrefix, threadId )
    return { statusCode: 200, body: "Image already exists" };
  }
  try {
    const getOriginalImageCommand = new GetObjectCommand({
      Bucket: S3_ORIGINAL_IMAGE_BUCKET,
      Key: originalImagePath,
    });
    const getOriginalImageCommandOutput = await s3Client.send(
      getOriginalImageCommand
    );
    originalImageBody =
      getOriginalImageCommandOutput.Body.transformToByteArray();
    contentType = getOriginalImageCommandOutput.ContentType;
  } catch (error) {
    throw new Error(error);
  }
  let transformedImage = Sharp(await originalImageBody, {
    failOn: "none",
    animated: true,
  });
  // Get image orientation to rotate if needed
  const imageMetadata = await transformedImage.metadata();
  // execute the requested operations
  const operationsJSON = Object.fromEntries(
    operationsPrefix.split(",").map((operation) => operation.split("="))
  );
  // variable holding the server timing header value
  let timingLog = "img-download;dur=" + parseInt(performance.now() - startTime);
  //   startTime = performance.now();
  try {
    // check if resizing is requested
    var resizingOptions = {};
    if (operationsJSON["width"])
      resizingOptions.width = parseInt(operationsJSON["width"]);
    if (operationsJSON["height"])
      resizingOptions.height = parseInt(operationsJSON["height"]);
    if (resizingOptions)
      transformedImage = transformedImage.resize(resizingOptions);
    // check if rotation is needed
    if (imageMetadata.orientation) transformedImage = transformedImage.rotate();
    // check if formatting is requested
    if (operationsJSON["format"]) {
      var isLossy = false;
      switch (operationsJSON["format"]) {
        case "jpeg":
          contentType = "image/jpeg";
          isLossy = true;
          break;
        case "gif":
          contentType = "image/gif";
          break;
        case "webp":
          contentType = "image/webp";
          isLossy = true;
          break;
        case "png":
          contentType = "image/png";
          break;
        case "avif":
          contentType = "image/avif";
          isLossy = true;
          break;
        default:
          contentType = "image/jpeg";
          isLossy = true;
      }
      if (operationsJSON["quality"] && isLossy) {
        transformedImage = transformedImage.toFormat(operationsJSON["format"], {
          quality: parseInt(operationsJSON["quality"]),
        });
      } else
        transformedImage = transformedImage.toFormat(operationsJSON["format"]);
    } else {
      /// If not format is precised, Sharp converts svg to png by default https://github.com/aws-samples/image-optimization/issues/48
      if (contentType === "image/svg+xml") contentType = "image/png";
    }
    transformedImage = await transformedImage.toBuffer();
  } catch (error) {
    throw new Error(error);
  }
  timingLog =
    timingLog + ",img-transform;dur=" + parseInt(performance.now() - startTime);

  // handle gracefully generated images bigger than a specified limit (e.g. Lambda output object limit)
  const imageTooBig = Buffer.byteLength(transformedImage) > MAX_IMAGE_SIZE;

  // upload transformed image back to S3 if required in the architecture
  if (S3_TRANSFORMED_IMAGE_BUCKET) {
    startTime = performance.now();
    try {
      const putImageCommand = new PutObjectCommand({
        Body: transformedImage,
        Bucket: S3_TRANSFORMED_IMAGE_BUCKET,
        Key: originalImagePath + "/" + operationsPrefix,
        ContentType: contentType,
        CacheControl: TRANSFORMED_IMAGE_CACHE_TTL,
      });
      await s3Client.send(putImageCommand);
      timingLog =
        timingLog +
        ",img-upload;dur=" +
        parseInt(performance.now() - startTime);
      console.log(
        "image uploaded:",
        originalImagePath + "/" + operationsPrefix,"threadId:", threadId
      );
      // If the generated image file is too big, send a redirection to the generated image on S3, instead of serving it synchronously from Lambda.
      if (imageTooBig) {
        return {
          statusCode: 302,
          headers: {
            Location:
              "/" +
              originalImagePath +
              "?" +
              operationsPrefix.replace(/,/g, "&"),
            "Cache-Control": "private,no-store",
            "Server-Timing": timingLog,
          },
        };
      }
    } catch (error) {
      throw new Error(error)
    }
  }

  // Return error if the image is too big and a redirection to the generated image was not possible, else return transformed image
  if (imageTooBig) {
    return sendError(403, "Requested transformed image is too big", "");
  } else
    return {
      statusCode: 200,
      body: transformedImage.toString("base64"),
      isBase64Encoded: true,
      headers: {
        "Content-Type": contentType,
        "Cache-Control": TRANSFORMED_IMAGE_CACHE_TTL,
        "Server-Timing": timingLog,
      },
    };
}

// Main execution
if (isMainThread) {
  console.log("inside main thread");
  const data = fs.readFileSync("./image-array.json", "utf-8"); // Read the file synchronously
  const jsonData = JSON.parse(data); // Parse JSON content
  const unprocessedImages = jsonData.filter((img) => !img.image_processed);
  console.log(
    `Starting processing of ${unprocessedImages.length} images using ${NUM_CORES} cores`
  );
  const startTime = performance.now();

  processImagesInParallel(unprocessedImages)
    .then((results) => {
      const timeElapsed = (performance.now() - startTime) / 1000;
      console.log(`Completed processing in ${timeElapsed.toFixed(2)} seconds`);

      // Log any errors
      const errors = results.filter((r) => !r.success);
      if (errors.length > 0) {
        console.log("Errors encountered:", errors);
      }
    })
    .catch((error) => {
      console.error("Fatal error:", error);
      process.exit(1);
    });

}

// async function handleExit(signal) {
//     console.log(`Received ${signal}, cleaning up and exiting...`);

//     const data = await readFileAsync("./image-array.json", "utf-8"); // Read the file synchronously
//     const jsonData = await JSON.parse(data); // Parse JSON content
//     await writeFileAsync(
//         "./image-array.json",
//         JSON.stringify(jsonData, null, 2),
//         "utf-8"
//       );
//     console.log('Cleanup complete, exiting process.');
//     process.exit(0); // Exit after async tasks are done
// }

// process.on('SIGINT', handleExit);
// process.on('SIGTERM', handleExit);