package amqp_lib

import (
	"fmt"

	"errors"

	"context"

	"github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

type AMQPQueueDefiner func(AMQPSession) error
type AMQPConsumerDefiner func(AMQPSession) (<-chan amqp.Delivery, error)

type AMQPSubscriberListener interface {
	OnConnect()
	OnReconnect()
	OnDeliveryReceived(AMQPSession, amqp.Delivery)
}

type AMQPSubscriber struct {
	listener AMQPSubscriberListener
}

func NewAMQPSubscriber(url string, listen AMQPSubscriberListener, queueDefiner AMQPQueueDefiner, consumerDefiner AMQPConsumerDefiner, l *logrus.Logger, ctx context.Context) *AMQPSubscriber {
	sub := &AMQPSubscriber{
		listener: listen,
	}

	go func() {
		var (
			err           error
			connectedOnce = false
		)
		conn := NewAMQPConnector(l, ctx)
		connections := conn.Connect(url)
		for conn := range connections {
			sess, ok := <-conn

			if !ok {
				l.Error("Subscribe queue error")
				continue
			}

			// notify listener
			if listen != nil {
				if connectedOnce {
					listen.OnReconnect()
				} else {
					connectedOnce = true
					listen.OnConnect()
				}
			}

			if queueDefiner != nil {
				err = queueDefiner(sess)
				if err != nil {
					l.WithField("error", err).Error("Error during define queue")
					continue
				}
			}

			var deliveries <-chan amqp.Delivery
			deliveries, err = consumerDefiner(sess)
			if err != nil {
				l.WithField("error", err).Error("Error during define consumer")
			}

			working := true
			for working {
				select {
				case msg := <-deliveries:
					// empty message, looks like connection was gone, need reconnect
					if msg.Type == "" && len(msg.Body) == 0 {
						working = false
						break
					}

					if listen != nil {
						listen.OnDeliveryReceived(sess, msg)
					}
				case <-ctx.Done():
					l.Info("Shut down subscriber")
					return
				}
			}
		}
	}()

	return sub
}

func AMQPExclusiveQueue(queue string, exchange string) AMQPQueueDefiner {
	return func(sub AMQPSession) error {
		if _, err := sub.QueueDeclare(queue, false, false, true, false, nil); err != nil {
			return errors.New(fmt.Sprintf("Cannot consume from exclusive queue: %q, %v", queue, err))
		}

		routingKey := ""
		if err := sub.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
			return errors.New(fmt.Sprintf("Cannot consume without a binding to exchange: %q, %v", exchange, err))
		}

		return nil
	}
}

func AMQPNoprefetchConsumer(queue string) AMQPConsumerDefiner {
	return func(sub AMQPSession) (<-chan amqp.Delivery, error) {
		err := sub.Qos(1, 0, false)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Cannot set Qos, %v", err))
		}

		deliveries, err := sub.Consume(queue, "", false, true, false, false, nil)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Cannot consume from: %q, %v", queue, err))
		}

		return deliveries, err
	}
}
