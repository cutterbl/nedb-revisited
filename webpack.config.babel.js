const pkg = require('./package.json');

const moduleName = pkg.name;

module.exports = (env, argv) => {
  return {
    entry: `${__dirname}/src/index.js`,
    output: {
      path: `${__dirname}/dist`,
      filename: `${moduleName}.js`,
      library: 'nedb',
      libraryExport: 'default',
      libraryTarget: 'umd',
      umdNamedDefine: true
    },
    optimization: {
      minimize: false
    },
    node: {
      fs: 'empty'
    },
    module: {
      rules: [
        {
          test: /\.js$/,
          exclude: /node_modules/,
          use: [
            {
              loader: 'babel-loader'
            },
            {
              loader: 'eslint-loader'
            }
          ]
        }
      ]
    },
    resolve: {
      modules: [`${__dirname}/node_modules`, `${__dirname}/src`],
      extensions: ['.js', '.json']
    }
  };
};
