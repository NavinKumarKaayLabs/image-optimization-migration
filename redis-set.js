const Redis = require("ioredis");
const redis = new Redis(); // Default connection to localhost:6379
const imageArray = require("./image-array.json");
const fs = require("fs");
const { promisify } = require("util");

const writeFileAsync = promisify(fs.writeFile);

const mergeFileToRedis = async () => {
  try {
    await redis.set("imageArray", JSON.stringify(imageArray));
    console.log("merged file to redis");
  } catch (error) {
    console.error("mergeRedisToFile error:", error);
  }
};

const getImageDataInFile = async (id) => {
  const initialValue = { optimizedImageLength: 0, notOptimizedImageLength: 0 };
  console.log(
    "get image data in file",
    imageArray.reduce((accumulator, currentValue) => {
      const returnObject = {};
      returnObject.optimizedImageLength =
        accumulator.optimizedImageLength +
        (currentValue.image_processed ? 1 : 0);
      returnObject.notOptimizedImageLength =
        accumulator.notOptimizedImageLength +
        (currentValue.image_processed ? 0 : 1);
      return returnObject;
    }, initialValue)
  );
  process.exit(1)
};

const getImageDataInRedis = async () => {
  const data = await redis.get("imageArray"); // Get array from Redis
  if (!data) {
    console.log("No data found!");
    return;
  }
  let array = JSON.parse(data); // Convert string back to array
  const initialValue = { optimizedImageLength: 0, notOptimizedImageLength: 0 };
  console.log(
    "get image data from redis",
    array.reduce((accumulator, currentValue) => {
      const returnObject = {};
      returnObject.optimizedImageLength =
        accumulator.optimizedImageLength +
        (currentValue.image_processed ? 1 : 0);
      returnObject.notOptimizedImageLength =
        accumulator.notOptimizedImageLength +
        (currentValue.image_processed ? 0 : 1);
      return returnObject;
    }, initialValue)
  );
  process.exit(1)
};

const mergeRedisToFile = async () => {
  try {
    const data = await redis.get("imageArray"); // Get array from Redis
    if (!data) {
      console.log("No data found!");
      return;
    }
    let array = JSON.parse(data); // Convert
    await writeFileAsync(
      "./image-array.json",
      JSON.stringify(array, null, 2),
      "utf-8"
    );
    console.log("data merged to file");
  } catch (error) {
    console.log("mergeRedisToFile error:", error);
  }
};

console.log(process.argv, process.argv[2]);

if (process.argv[2] === "mergeRedisToFile") {
  mergeRedisToFile();
} else if (process.argv[2] === "mergeFileToRedis") {
  mergeFileToRedis();
} else if (process.argv[2] === "getImageDataInRedis") {
  getImageDataInRedis();
} else if (process.argv[2] === "getImageDataInFile") {
  getImageDataInFile();
}
