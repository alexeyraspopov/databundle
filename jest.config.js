module.exports = {
  collectCoverage: true,
  transform: {
    ".js$": "babel-jest",
  },
  transformIgnorePatterns: ["node_modules/(?!(d3-array|internmap)/)"],
};
