const amqp = require('amqp');
const EventEmitter = require('events');

let logger
try {
  logger = require('./log').getLogger('amqp');
} catch (error) {
  logger = console
}


const MQTYPE = {
  RPC_REQ: 1,
  RPC_RES: 2
}


module.exports = class AMQPWrapper extends EventEmitter {
  /**
   *
   *
   * @param {string} amqpUrl
   * @param {string} exchangeName
   * @memberof AMQPWrapper
   */
  createConnection(amqpUrl, exchangeName) {
    this.amqpConn = amqp.createConnection({ url: amqpUrl, heartbeat: 30 }, { reconnect: true });
    this.amqpConn.on('error', (err) => { logger.error('amqp error, ', err); });
    this.amqpConn.on('ready', () => {
      if (this.exchange) return;
      logger.info("amqp will connect to ", exchangeName);
      this.exchange = this.amqpConn.exchange(exchangeName, { passive: true, }, (exchange) => {
        logger.info('Exchange %s is open', exchange.name);
        this.emit('ready');
      });
    });
  }

  /**
   * subscribe a queue and bind a route
   *
   * @param {string} queue
   * @param {string} route
   * @memberof AMQPWrapper
   */
  subscribe(queue, route) {
    let exchangeName = this.exchange.name;
    //this.amqpConn.queue(queue, { passive: false, durable: true, autoDelete: false },
    this.amqpConn.queue(queue,
      (q) => {
        logger.info('open queue %s success', queue);
        q.bind(exchangeName, route);
        q.subscribe((message, headers, deliveryInfo, messageObject) => {
          logger.debug('got message %j', message);
          this.emit(queue, message);
        });
      });
  }

  /**
   * publish msg by route
   *
   * @param {string} route
   * @param {*} msg
   * @memberof AMQPWrapper
   */
  publish(route, msg) {
    if (this.exchange) this.exchange.publish(route, msg)
  }


  onRpcMsg(msg) {
    try {
      switch (message.type) {
      case MQTYPE.RPC_RES: // rpc message
        {
          let rpc = this.rpc.get(uuid);
          this.rpc.delete(uuid);
          clearTimeout(rpc.timer);

          if (msg.code === 0) {
            rpc.callback(null, msg.result);
          } else {
            rpc.callback(Error(msg.error));
          }
        }
        break;
      default: // normal message
        callback(message);
      }
    } catch (error) {
      logger.error("mq msg error, ", error);
    }
  }

  callRpc(msg, route, callback) {
    let uuid = msg.uuid = randomString(12);
    msg.type = MQTYPE.RPC_REQ;
    let timer = setTimeout(() => {
      this.rpc.delete(uuid);
      callback(Error(`Timeout. ${uuid}`));
    }, 5 * 1000);

    let rpc = {
      callback: callback,
      timer: timer
    }

    this.rpc.set(uuid, rpc);
    this.publish(route, msg);
  }
}

function randomString(len) {
  len = len || 32;
  var $chars = 'ABCDEFGHJKMNPQRSTWXYZabcdefhijkmnprstwxyz2345678'; /****默认去掉了容易混淆的字符oOLl,9gq,Vv,Uu,I1****/
  var maxPos = $chars.length;
  var pwd = '';
  for (let i = 0; i < len; i++) {
    pwd += $chars.charAt(Math.floor(Math.random() * maxPos));
  }
  return pwd;
}

// module.exports = AMQPWrapper;
// const mq = new AMQPWrapper();

// mq.createConnection('amqp://admin:admin@192.168.60.252:5672/convert', 'status-exchange');
// mq.on('ready', () => {
//     mq.subscribe('qa', 'ka');
//     mq.subscribe('qb', 'kb');
//     mq.publish('ka', { msg: 'hello' })
//   })
//   .on('qa', msg => {
//     logger.info('qa recv: ', msg)
//     mq.publish('kb', "ssss")
//   })
//   .on('qb', msg => {
//     logger.info('qb recv: ', msg)
//   });

// 同时两个个会有异常
// const mq2 = new AMQPWrapper();

// mq2.createConnection('amqp://admin:admin@192.168.60.252:5672/convert', 'av_exchange');
// mq2.on('ready', () => {
// logger.info('mq2 ready')
// mq.subscribe('qa2', 'ka2');
// mq.subscribe('qb2', 'kb2');
// mq.publish('ka2', { msg: 'helloxxx' })
// })
// .on('qa2', msg => {
// logger.info('qa2 recv: ', msg)
// mq.publish('kb2', "ssssxxx")
// })
// .on('qb2', msg => {
// logger.info('qb2 recv: ', msg)
// });