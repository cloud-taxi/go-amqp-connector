package amqp_lib

import (
	"time"

	"context"

	"github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

const (
	RECONNECT_TIMEOUT = time.Second * 3
)

type AMQPSession struct {
	*amqp.Connection
	*amqp.Channel
}

type AMQPConnector struct {
	ctx context.Context
	l   *logrus.Logger
}

func (ac *AMQPConnector) Connect(url string) chan chan AMQPSession {
	sessions := make(chan chan AMQPSession)

	go func() {
		defer close(sessions)
		for {
			fnDone := make(chan bool)
			go func() {
				ac.connectCycle(sessions, url)
				close(fnDone)
			}()

			select {
			case <-fnDone:
			case <-ac.ctx.Done():
				ac.l.Info("Shutting down connector")
				return
			}

			time.Sleep(RECONNECT_TIMEOUT)
		}
	}()

	return sessions
}

func (ac *AMQPConnector) connectCycle(sessions chan chan AMQPSession, url string) {
	sess := make(chan AMQPSession)

	// make connection to reader
	// "sessions" is not buffered channel, so we will wait until some reader appears
	select {
	case sessions <- sess:
	case <-ac.ctx.Done():
		ac.l.Info("Shutting down connection")
		close(sess)
		return
	}

	ac.l.WithField("url", url).Info("Dial rabbitmq")

	conn, err := amqp.Dial(url)
	if err != nil {
		ac.l.WithFields(logrus.Fields{
			"error": err,
			"url":   url,
		}).Error("Cannot (re)dial")
		close(sess)
		return
	}

	ch, err := conn.Channel()
	if err != nil {
		ac.l.WithField("error", err).Error("Cannot create channel")
		close(sess)
		return
	}

	select {
	case sess <- AMQPSession{conn, ch}:
	case <-ac.ctx.Done():
		ac.l.Info("Shutting down new session")
		close(sess)
		return
	}

	return
}

func NewAMQPConnector(log *logrus.Logger, c context.Context) *AMQPConnector {
	return &AMQPConnector{
		ctx: c,
		l:   log,
	}
}
