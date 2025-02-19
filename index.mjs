// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
import fs from 'fs';
import { GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import Sharp from 'sharp';
import dotenv from "dotenv";
import imageArray from './image-array.json' assert { type: 'json'}
// const imageArray = require('./image-array.json')

dotenv.config();
const s3Client = new S3Client({
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
      },
      region: process.env.AWS_DEFAULT_REGION || 'ap-south-1'
});
const S3_ORIGINAL_IMAGE_BUCKET = process.env.originalImageBucketName;
const S3_TRANSFORMED_IMAGE_BUCKET = process.env.transformedImageBucketName;
const TRANSFORMED_IMAGE_CACHE_TTL = process.env.transformedImageCacheTTL;
const MAX_IMAGE_SIZE = parseInt(process.env.maxImageSize);

export const handler = async (event) => {
    // Validate if this is a GET request
    // An example of expected path is /images/rio/1.jpeg/format=auto,width=100 or /images/rio/1.jpeg/original where /images/rio/1.jpeg is the path of the original image
    var imagePathArray = decodeURIComponent(new URL(event).pathname).split('/')
    const searchPath =  new URL(event).search;
    // get the requested image operations
    var operationsPrefix = searchPath ? searchPath.substring(1, searchPath.length ) : 'original';
    imagePathArray.pop();
    // console.log("operationsPrefix", operationsPrefix)
    // get the original image path images/rio/1.jpg
    imagePathArray.shift();
    var originalImagePath = imagePathArray.join('/');
    // console.log("originalImagePath", originalImagePath)
    var startTime = performance.now();
    // Downloading original image
    let originalImageBody;
    let contentType;

    const imageExist = async () =>{
        try {
            const getOriginalImageCommand = new GetObjectCommand({ Bucket: S3_TRANSFORMED_IMAGE_BUCKET, Key: originalImagePath + '/' + operationsPrefix });
            const getOriginalImageCommandOutput = await s3Client.send(getOriginalImageCommand);
    
            console.log('Got response from S3 for' , originalImagePath +  '/' + operationsPrefix );
            contentType = getOriginalImageCommandOutput.ContentType;
        return true;
        } catch (error) {
            return false;
        }
        }
        const checkImageExist = await imageExist();
        if(checkImageExist){
            return console.log("image aleady exist")
            // sendError(500, '<--------image already exist--------->', originalImagePath +  '/' + operationsPrefix)
        }

    try {
        const getOriginalImageCommand = new GetObjectCommand({ Bucket: S3_ORIGINAL_IMAGE_BUCKET, Key: originalImagePath });
        const getOriginalImageCommandOutput = await s3Client.send(getOriginalImageCommand);

        console.log(`Got response from S3 for ${originalImagePath}`);
        // throw new Error('error')

        originalImageBody = getOriginalImageCommandOutput.Body.transformToByteArray();
        contentType = getOriginalImageCommandOutput.ContentType;

    } catch (error) {
        return sendError(500, 'Error downloading original image--------->',error);
    }
    let transformedImage = Sharp(await originalImageBody, { failOn: 'none', animated: true });
    // Get image orientation to rotate if needed
    const imageMetadata = await transformedImage.metadata();
    // execute the requested operations 
    const operationsJSON = Object.fromEntries(operationsPrefix.split(',').map(operation => operation.split('=')));
    // variable holding the server timing header value
    var timingLog = 'img-download;dur=' + parseInt(performance.now() - startTime);
    startTime = performance.now();
    try {
        // check if resizing is requested
        var resizingOptions = {};
        if (operationsJSON['width']) resizingOptions.width = parseInt(operationsJSON['width']);
        if (operationsJSON['height']) resizingOptions.height = parseInt(operationsJSON['height']);
        if (resizingOptions) transformedImage = transformedImage.resize(resizingOptions);
        // check if rotation is needed
        if (imageMetadata.orientation) transformedImage = transformedImage.rotate();
        // check if formatting is requested
        if (operationsJSON['format']) {
            var isLossy = false;
            switch (operationsJSON['format']) {
                case 'jpeg': contentType = 'image/jpeg'; isLossy = true; break;
                case 'gif': contentType = 'image/gif'; break;
                case 'webp': contentType = 'image/webp'; isLossy = true; break;
                case 'png': contentType = 'image/png'; break;
                case 'avif': contentType = 'image/avif'; isLossy = true; break;
                default: contentType = 'image/jpeg'; isLossy = true;
            }
            if (operationsJSON['quality'] && isLossy) {
                transformedImage = transformedImage.toFormat(operationsJSON['format'], {
                    quality: parseInt(operationsJSON['quality']),
                });
            } else transformedImage = transformedImage.toFormat(operationsJSON['format']);
        } else {
            /// If not format is precised, Sharp converts svg to png by default https://github.com/aws-samples/image-optimization/issues/48
            if (contentType === 'image/svg+xml') contentType = 'image/png';
        }
        transformedImage = await transformedImage.toBuffer();
    } catch (error) {
        return sendError(500, 'error transforming image', error);
    }
    timingLog = timingLog + ',img-transform;dur=' + parseInt(performance.now() - startTime);

    // handle gracefully generated images bigger than a specified limit (e.g. Lambda output object limit)
    const imageTooBig = Buffer.byteLength(transformedImage) > MAX_IMAGE_SIZE;
  

    // upload transformed image back to S3 if required in the architecture
    if (S3_TRANSFORMED_IMAGE_BUCKET && !checkImageExist) {
        startTime = performance.now();
        try {
            const putImageCommand = new PutObjectCommand({
                Body: transformedImage,
                Bucket: S3_TRANSFORMED_IMAGE_BUCKET,
                Key: originalImagePath + '/' + operationsPrefix,
                ContentType: contentType,
                CacheControl: TRANSFORMED_IMAGE_CACHE_TTL,
            })
            await s3Client.send(putImageCommand);
            timingLog = timingLog + ',img-upload;dur=' + parseInt(performance.now() - startTime);
            console.log("image uploaded:", originalImagePath + '/' + operationsPrefix)
            // If the generated image file is too big, send a redirection to the generated image on S3, instead of serving it synchronously from Lambda. 
            if (imageTooBig) {
                return {
                    statusCode: 302,
                    headers: {
                        'Location': '/' + originalImagePath + '?' + operationsPrefix.replace(/,/g, "&"),
                        'Cache-Control': 'private,no-store',
                        'Server-Timing': timingLog
                    }
                };
            }
        } catch (error) {
            logError('Could not upload transformed image to S3', error);
        }
    }

    // Return error if the image is too big and a redirection to the generated image was not possible, else return transformed image
    if (imageTooBig) {
        return sendError(403, 'Requested transformed image is too big', '');
    } else return {
        statusCode: 200,
        body: transformedImage.toString('base64'),
        isBase64Encoded: true,
        headers: {
            'Content-Type': contentType,
            'Cache-Control': TRANSFORMED_IMAGE_CACHE_TTL,
            'Server-Timing': timingLog
        }
    };
};

function sendError(statusCode, body, error) {
    logError(body, error);
    return { statusCode, body };
}

function logError(body, error) {
    console.log('APPLICATION ERROR', body, error);
    // console.log(error);
}


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
    'original'
  ];

// const 
// Assuming imageSizes is an array of sizes you want to iterate over
for (const [imageIndex, image] of imageArray.entries()) {
    const startTimeImageTime = performance.now();
    if(!image.image_processed){
    for (const size of imageSizes) {
      // Construct the query string based on the image size
      const appendQuery = size === 'original' ? 'original' : `?format=avif,width=${size}`;
      // Log the image and query string
      console.log(`${image.product_image}/${appendQuery}`);
      // Wait for the handler function to complete for each image/size combination
    await handler(decodeURIComponent(`${image.product_image}/${appendQuery}`));
    const item = imageArray.find(findImage => (findImage.id === imageIndex + 1 && findImage.product_image === image.product_image));
    if (item) {
    item.image_processed = true;
    }
    fs.writeFileSync('./image-array.json',JSON.stringify(imageArray, null, 2), 'utf-8' )
    }
    }
    console.log(`Time taken for image: ${imageArray.length} - ${imageIndex + 1}>`, performance.now() - startTimeImageTime, 'ms');
  }
  