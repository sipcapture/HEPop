const getRuntimePlatformArch = () => `${process.platform}-${process.arch}`;

/**
 * @throw Error if there isn't any available native binding for the current platform/arch.
 */
const getNativeNodeBinding = (runtimePlatformArch) => {
    return require(`@duckdb/node-bindings-${process.platform}-${process.arch}/duckdb.node`);
}

module.exports = getNativeNodeBinding(getRuntimePlatformArch());

