const imageArray  = require('./image-array.json')

const initialValue = {"optimizedImageLength": 0, "notOptimizedImageLength": 0} 

console.log("image Array", imageArray.length, imageArray.reduce((accumulator, currentValue) => {
    const returnObject = {}
    returnObject.optimizedImageLength = accumulator.optimizedImageLength + (currentValue.image_processed ? 1 : 0)
    returnObject.notOptimizedImageLength = accumulator.notOptimizedImageLength + (currentValue.image_processed ? 0 : 1)
    return returnObject;
    },
initialValue))