const lz4 = require('lz4')


/**
 * @param {*} rawData uncompressed Buffer
 * @returns Tuple: {compressed Buffer, original size of uncompressed Buffer}
 */
function compress(rawData) {
    const output = Buffer.alloc(lz4.encodeBound(rawData.length));
    const compressedSize = lz4.encodeBlock(rawData, output);
    const compressed = output.slice(0, compressedSize);
    return {compressed, size: rawData.length};
}

/**
 * @param {*} compressedData Buffer
 * @param {*} size original size of uncompressed Buffer
 * @returns uncompressed Buffer
 */
function deCompress(compressedData, size) {
    const input = Buffer.from(compressedData);
    const output = Buffer.alloc(size); 
    const decompressedSize = lz4.decodeBlock(input, output);
    const raw = output.slice(0, decompressedSize);
    return raw;
}

module.exports = {
    compress,
    deCompress
};