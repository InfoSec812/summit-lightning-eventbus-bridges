var
  shell = require('shelljs'),
  path = require('path')

shell.rm('-rf', path.resolve(__dirname, '../../resources/webroot/*'))
shell.rm('-rf', path.resolve(__dirname, '../../resources/webroot/.*'))
console.log(' Cleaned build artifacts.\n')
