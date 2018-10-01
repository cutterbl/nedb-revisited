module.exports = (env, argv) => {
  return {
    entry: `${__dirname}/index.js`,
    output: {
      path: `${__dirname}/dist`,
      filename: 'nedb-revisited.js'
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
        },
        {
          test: /\.json$/,
          exclude: /node_modules/,
          use: {
            loader: 'json-loader'
          }
        }
      ]
    },
    resolve: {
      modules: [`${__dirname}/node_modules`, `${__dirname}/src`],
      extensions: ['.js', '.json']
    }
  };
};
