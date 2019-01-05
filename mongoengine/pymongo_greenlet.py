import atexit
import functools
import socket
import warnings
import weakref
import time

# So that 'setup.py doc' can import this module without Tornado or greenlet
requirements_satisfied = True
try:
    from tornado import iostream, ioloop
except ImportError:
    requirements_satisfied = False
    warnings.warn("Tornado not installed", ImportWarning)

try:
    import greenlet
except ImportError:
    requirements_satisfied = False
    warnings.warn("greenlet module not installed", ImportWarning)


import pymongo
import pymongo.common
import pymongo.errors
import pymongo.mongo_client
import pymongo.mongo_replica_set_client
import pymongo.pool
import pymongo.son_manipulator
import logging

class MongoIOStream(iostream.IOStream):
    def can_read_sync(self, num_bytes):
        return self._read_buffer_size >= num_bytes

def _check_deadline(cleanup_cb=None):
    gr = greenlet.getcurrent()
    if hasattr(gr, 'is_deadlined') and \
            gr.is_deadlined():
        if cleanup_cb:
            cleanup_cb()
        try:
            gr.do_deadline()
        except AttributeError:
            logging.exception(
                'Greenlet %s has \'is_deadlined\' but not \'do_deadline\'')

def green_sock_method(method):
    """Wrap a GreenletSocket method to pause the current greenlet and arrange
       for the greenlet to be resumed when non-blocking I/O has completed.
    """
    @functools.wraps(method)
    def _green_sock_method(self, *args, **kwargs):
        self.child_gr = greenlet.getcurrent()
        main = self.child_gr.parent
        assert main, "Should be on child greenlet"

        # Run on main greenlet
        def closed(gr):
            # The child greenlet might have died, e.g.:
            # - An operation raised an error within PyMongo
            # - PyMongo closed the MotorSocket in response
            # - GreenletSocket.close() closed the IOStream
            # - IOStream scheduled this closed() function on the loop
            # - PyMongo operation completed (with or without error) and
            #       its greenlet terminated
            # - IOLoop runs this function
            if not gr.dead:
                gr.throw(socket.error("Close called, killing mongo operation"))

        # send the error to this greenlet if something goes wrong during the
        # query
        self.stream.set_close_callback(functools.partial(closed, self.child_gr))

        try:
            # Add timeout for closing non-blocking method call
            if self.timeout and not self.timeout_handle:
                self.timeout_handle = self.io_loop.add_timeout(
                    time.time() + self.timeout, self._switch_and_close)

            # method is GreenletSocket.send(), recv(), etc. method() begins a
            # non-blocking operation on an IOStream and arranges for
            # callback() to be executed on the main greenlet once the
            # operation has completed.
            method(self, *args, **kwargs)

            # Pause child greenlet until resumed by main greenlet, which
            # will pass the result of the socket operation (data for recv,
            # number of bytes written for sendall) to us.
            socket_result = main.switch()

            return socket_result
        except socket.error:
            raise
        except IOError, e:
            # If IOStream raises generic IOError (e.g., if operation
            # attempted on closed IOStream), then substitute socket.error,
            # since socket.error is what PyMongo's built to handle. For
            # example, PyMongo will catch socket.error, close the socket,
            # and raise AutoReconnect.
            raise socket.error(str(e))
        finally:
            # do this here in case main.switch throws

            # Remove timeout handle if set, since we've completed call
            if self.timeout_handle:
                self.io_loop.remove_timeout(self.timeout_handle)
                self.timeout_handle = None

            # disable the callback to raise exception in this greenlet on socket
            # close, since the greenlet won't be around to raise the exception
            # in (and it'll be caught on the next query and raise an
            # AutoReconnect, which gets handled properly)
            self.stream.set_close_callback(None)

            def cleanup_cb():
                self.stream.close()
                try:
                    self.pool_ref._socket_semaphore.release()
                except weakref.ReferenceError:
                    # pool was gc'ed
                    pass

            _check_deadline(cleanup_cb)

    return _green_sock_method


class GreenletSocket(object):
    """Replace socket with a class that yields from the current greenlet, if
    we're on a child greenlet, when making blocking calls, and uses Tornado
    IOLoop to schedule child greenlet for resumption when I/O is ready.

    We only implement those socket methods actually used by pymongo.
    """
    def __init__(self, sock, io_loop, use_ssl=False, pool_ref=None):
        self.use_ssl = use_ssl
        self.io_loop = io_loop
        self.timeout = None
        self.timeout_handle = None
        self.pool_ref = pool_ref
        if self.use_ssl:
            raise Exception("SSL isn't supported")
        else:
            self.stream = MongoIOStream(sock, io_loop=io_loop)

    def setsockopt(self, *args, **kwargs):
        self.stream.socket.setsockopt(*args, **kwargs)

    def settimeout(self, timeout):
        self.timeout = timeout

    def _switch_and_close(self):
        # called on timeout to switch back to child greenlet
        self.close()
        if self.child_gr is not None:
            self.child_gr.throw(IOError("Socket timed out"))

    @green_sock_method
    def connect(self, pair):
        # do the connect on the underlying socket asynchronously...
        self.stream.connect(pair, greenlet.getcurrent().switch)

    @green_sock_method
    def sendall(self, data):
        try:
            self.stream.write(data, greenlet.getcurrent().switch)
        except IOError as e:
            raise socket.error(str(e))

        if self.stream.closed():
            raise socket.error("connection closed")

    def recv(self, num_bytes):
        # if we have enough bytes in our local buffer, don't yield
        if self.stream.can_read_sync(num_bytes):
            return self.stream._consume(num_bytes)
        # else yield while we wait on Mongo to send us more
        else:
            return self.recv_async(num_bytes)

    @green_sock_method
    def recv_async(self, num_bytes):
        # do the recv on the underlying socket... come back to the current
        # greenlet when it's done
        return self.stream.read_bytes(num_bytes, greenlet.getcurrent().switch)

    def close(self):
        # since we're explicitly handling closing here, don't raise an exception
        # via the callback
        self.stream.set_close_callback(None)

        sock = self.stream.socket
        try:
            try:
                self.stream.close()
            except KeyError:
                # Tornado's _impl (epoll, kqueue, ...) has already removed this
                # file descriptor from its dict.
                pass
        finally:
            # Sometimes necessary to avoid ResourceWarnings in Python 3:
            # specifically, if the fd is closed from the OS's view, then
            # stream.close() throws an exception, but the socket still has an
            # fd and so will print a ResourceWarning. In that case, calling
            # sock.close() directly clears the fd and does not raise an error.
            if sock:
                sock.close()

    def fileno(self):
        return self.stream.socket.fileno()


class GreenletPool(pymongo.pool.Pool):
    """A simple connection pool of GreenletSockets.
    """
    def __init__(self, *args, **kwargs):
        io_loop = kwargs.pop('io_loop', None)
        self.io_loop = io_loop if io_loop else ioloop.IOLoop.instance()
        pymongo.pool.Pool.__init__(self, *args, **kwargs)

        if self.max_size is not None and self.wait_queue_multiple:
            raise ValueError("GreenletPool doesn't support wait_queue_multiple")

        # HACK [adam Dec/6/14]: need to use our IOLoop/greenlet semaphore
        #      implementation, so override what Pool.__init__ sets
        #      self._socket_semaphore to here
        self._socket_semaphore = GreenletBoundedSemaphore(self.max_size)

    def create_connection(self):
        """Copy of BasePool.connect()
        """
        assert greenlet.getcurrent().parent, "Should be on child greenlet"

        host, port = self.pair

        # Don't try IPv6 if we don't support it. Also skip it if host
        # is 'localhost' (::1 is fine). Avoids slow connect issues
        # like PYTHON-356.
        family = socket.AF_INET
        if socket.has_ipv6 and host != 'localhost':
            family = socket.AF_UNSPEC

        err = None
        for res in socket.getaddrinfo(host, port, family, socket.SOCK_STREAM):
            af, socktype, proto, dummy, sa = res
            green_sock = None
            try:
                sock = socket.socket(af, socktype, proto)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                green_sock = GreenletSocket(
                    sock, self.io_loop, use_ssl=self.use_ssl,
                    pool_ref=weakref.proxy(self))

                # GreenletSocket will pause the current greenlet and resume it
                # when connection has completed
                green_sock.settimeout(self.conn_timeout)
                green_sock.connect(sa)
                green_sock.settimeout(self.net_timeout)
                return green_sock
            except socket.error, e:
                err = e
                if green_sock is not None:
                    green_sock.close()

        if err is not None:
            # pylint: disable=E0702
            raise err
        else:
            # This likely means we tried to connect to an IPv6 only
            # host with an OS/kernel or Python interpeter that doesn't
            # support IPv6.
            raise socket.error('getaddrinfo failed')


class GreenletEvent(object):
    def __init__(self, io_loop):
        self.io_loop = io_loop

        self._flag = False
        self._waiters = []

    def is_set(self):
        return self._flag

    isSet = is_set

    def set(self):
        self._flag = True
        waiters, self._waiters = self._waiters, []

        # wake up all the greenlets that were waiting
        for waiter in waiters:
            self.io_loop.add_callback(waiter.switch)

    def clear(self):
        self._flag = False

    def wait(self):
        current = greenlet.getcurrent()
        parent = current.parent
        assert parent, "Must be called on child greenlet"

        # yield back to the IOLoop if we have to wait
        if not self._flag:
            self._waiters.append(current)
            try:
                parent.switch()
            finally:
                # don't need callback because we haven't taken any resources
                _check_deadline()

        return self._flag

class GreenletSemaphore(object):
    """
        Tornado IOLoop+Greenlet-based Semaphore class
    """

    def __init__(self, value=1, io_loop=None):
        if value < 0:
            raise ValueError("semaphore initial value must be >= 0")
        self._value = value
        self._waiters = []
        self._waiter_timeouts = {}

        self._ioloop = io_loop if io_loop else ioloop.IOLoop.instance()

    def _handle_timeout(self, timeout_gr):
        if len(self._waiters) > 1000:
            import os
            logging.error('waiters size: %s on pid: %s', len(self._waiters),
                    os.getpid())
        # should always be there, but add some safety just in case
        if timeout_gr in self._waiters:
            self._waiters.remove(timeout_gr)

        if timeout_gr in self._waiter_timeouts:
            self._waiter_timeouts.pop(timeout_gr)

        timeout_gr.switch()

    def acquire(self, blocking=True, timeout=None):
        if not blocking and timeout is not None:
            raise ValueError("can't specify timeout for non-blocking acquire")

        current = greenlet.getcurrent()
        parent = current.parent
        assert parent, "Must be called on child greenlet"

        start_time = time.time()

        # if the semaphore has a postive value, subtract 1 and return True
        if self._value > 0:
            self._value -= 1
            return True
        elif not blocking:
            # non-blocking mode, just return False
            return False
        # otherwise, we don't get the semaphore...
        while True:
            self._waiters.append(current)
            if timeout:
                callback = functools.partial(self._handle_timeout, current)
                self._waiter_timeouts[current] = \
                        self._ioloop.add_timeout(time.time() + timeout,
                                                 callback)

            # yield back to the parent, returning when someone releases the
            # semaphore
            #
            # because of the async nature of the way we yield back, we're
            # not guaranteed to actually *get* the semaphore after returning
            # here (someone else could acquire() between the release() and
            # this greenlet getting rescheduled). so we go back to the loop
            # and try again.
            #
            # this design is not strictly fair and it's possible for
            # greenlets to starve, but it strikes me as unlikely in
            # practice.
            try:
                parent.switch()
            finally:
                # need to wake someone else up if we were the one
                # given the semaphore
                def _cleanup_cb():
                    if self._value > 0:
                        self._value -= 1
                        self.release()
                _check_deadline(_cleanup_cb)

            if self._value > 0:
                self._value -= 1
                if hasattr(current, '__mongoengine_comment__'):
                    current.add_mongo_start(
                        current.__mongoengine_comment__, time.time())
                return True

            # if we timed out, just return False instead of retrying
            if timeout and (time.time() - start_time) >= timeout:
                return False

    __enter__ = acquire

    def release(self):
        current = greenlet.getcurrent()
        if hasattr(current, '__mongoengine_comment__'):
            is_scatter_gather = False
            if hasattr(current, '__scatter_gather__'):
                is_scatter_gather = current.__scatter_gather__
            current.add_mongo_end(
                current.__mongoengine_comment__, time.time(),
                is_scatter_gather)

        self._value += 1

        if self._waiters:
            waiting_gr = self._waiters.pop(0)

            # remove the timeout
            if waiting_gr in self._waiter_timeouts:
                timeout = self._waiter_timeouts.pop(waiting_gr)
                self._ioloop.remove_timeout(timeout)

            # schedule the waiting greenlet to try to acquire
            self._ioloop.add_callback(waiting_gr.switch)

    def __exit__(self, t, v, tb):
        self.release()

    @property
    def counter(self):
        return self._value


class GreenletBoundedSemaphore(GreenletSemaphore):
    """Semaphore that checks that # releases is <= # acquires"""
    def __init__(self, value=1):
        GreenletSemaphore.__init__(self, value)
        self._initial_value = value

    def release(self):
        if self._value >= self._initial_value:
            raise ValueError("Semaphore released too many times")
        return GreenletSemaphore.release(self)


class GreenletPeriodicExecutor(object):
    _executors = set()

    def __init__(self, interval, dummy, target, io_loop):
        # dummy is in the place of min_interval which has no semantic
        # equivalent in this implementation
        self._interval = interval
        self._target = target
        self._io_loop = io_loop

        self._stopped = True
        self._next_timeout = None
        # make sure multiple calls to wake() only schedules once
        self._scheduled = False

    # i'm about 90% sure these three methods are pymongo's safeguard against
    # forgetting to close these things themselves
    @classmethod
    def _register_executor(cls, executor):
        ref = weakref.ref(executor, cls._on_executor_deleted)
        cls._executors.add(ref)

    @classmethod
    def _on_executor_deleted(cls, ref):
        cls._executors.remove(ref)

    @classmethod
    def _shutdown_executors(cls):
        executors = list(cls._executors)
        for ref in executors:
            executor = ref()
            if executor:
                executor.close()

    def open(self):
        if self._stopped:
            if not self._next_timeout and not self._scheduled:
                self._io_loop.add_callback(self._execute)
                self._scheduled = True
            self._stopped = False

    def wake(self):
        if not self._stopped:
            # schedule immediately
            self._cancel_next_run()
            if not self._scheduled:
                self._io_loop.add_callback(self._execute)
                self._scheduled = True

    def close(self, dummy=None):
        self._stopped = True
        self._cancel_next_run()

    def join(self, timeout=None):
        pass

    def _cancel_next_run(self):
        if self._next_timeout:
            self._io_loop.remove_timeout(self._next_timeout)

    def _execute(self):
        self._next_timeout = None
        self._scheduled = False
        # cover the case where close is called after wake
        if self._stopped:
            return

        try:
            if not self._target():
                self._stopped = True
                return
        except Exception:
            self._stopped = True
            # NOTE: this is an implementation difference from the real
            # PeriodicExecutor. the real one ends up killing the thread, while
            # this one propogates to the IOLoop handler.
            raise
        iotimeout = time.time() + self._interval
        self._next_timeout = self._io_loop.add_timeout(iotimeout,
                                                       self._execute)

atexit.register(GreenletPeriodicExecutor._shutdown_executors)


class GreenletLock(object):
    # we need to replace the internal lock do avoid the following scenario:
    # greenlet 1:
    # with lock:
    #     # do some io-blocking action, context switch to greenlet 2
    #
    # greenlet 2:
    # with lock: # deadlock
    #
    # we can't just replace it with an RLock:
    # greenlet 1:
    # with lock:
    #     # do some action only one thread of control is expected
    #     # do some io-blocking action, context switch to greenlet 2
    # greenlet 2:
    # with lock:
    #     # lock is granted, potentially corrupting state for greenlet 1

    # don't need to be too fancy or thread-safe because it's only coroutines
    def __init__(self, io_loop):
        # not an rlock, so we don't need to keep track of the holder,
        # but might as well for sanity-checking
        self.holder = None
        self.waiters = []
        self.io_loop = io_loop

    def acquire(self, blocking=True):
        current = greenlet.getcurrent()
        parent = current.parent
        assert parent, "Must be called on child greenlet"

        while self.holder:
            if blocking:
                self.waiters.append(current)
                parent.switch()
            else:
                return False

        self.holder = current

    def release(self):
        current = greenlet.getcurrent()
        assert self.holder is current, 'must be held'
        self.holder = None
        if self.waiters:
            waiter = self.waiters.pop(0)
            self.io_loop.add_callback(waiter.switch)

    def __enter__(self):
        self.acquire()

    def __exit__(self, *args):
        self.release()


class GreenletCondition(object):
    # replacement class for threading.Condition
    # only implements the methods used by pymongo.

    def __init__(self, io_loop, lock):
        self.lock = lock
        self.waiters = []
        self.waiter_timeouts = {}
        self.io_loop = io_loop

    def _handle_timeout(self, timeout_gr):
        self.waiters.remove(timeout_gr)
        self.waiter_timeouts.pop(timeout_gr)
        timeout_gr.switch()

    def wait(self, timeout=None):
        current = greenlet.getcurrent()
        parent = current.parent
        assert parent, "Must be called on child greenlet"
        assert self.lock.holder is current, 'must hold lock'

        # yield back to the IOLoop
        self.waiters.append(current)
        if timeout:
            callback = functools.partial(self._handle_timeout, current)
            iotimeout = timeout + time.time()
            self.waiter_timeouts[current] = self.io_loop.add_timeout(iotimeout,
                                                                     callback)
        self.lock.release()
        # we'll be returned to by the timeout or by notify_all
        parent.switch()

        self.lock.acquire()

    def notify_all(self):
        current = greenlet.getcurrent()
        assert self.lock.holder is current, 'must hold lock'
        waiters, self.waiters = self.waiters, []

        for waiter in waiters:
            self.io_loop.add_callback(waiter.switch)

            if waiter in self.waiter_timeouts:
                timeout = self.waiter_timeouts.pop(waiter)
                self.io_loop.remove_timeout(timeout)


class GreenletClient(object):
    client = None

    @classmethod
    def sync_connect(cls, *args, **kwargs):
        """
            Makes a synchronous connection to pymongo using Greenlets

            Fire up the IOLoop to do the connect, then stop it.
        """

        assert not greenlet.getcurrent().parent, "must be run on root greenlet"

        def _inner_connect(io_loop, *args, **kwargs):
            # asynchronously create a MongoClient using our IOLoop
            try:
                kwargs['use_greenlets'] = True
                kwargs['_pool_class'] = GreenletPool
                kwargs['_event_class'] = functools.partial(GreenletEvent,
                                                           io_loop)
                cls.client = pymongo.mongo_client.MongoClient(*args, **kwargs)
            except:
                logging.exception("Failed to connect to MongoDB")
            finally:
                io_loop.stop()

        # clear cls.client so we can't return an old one
        if cls.client is not None:
            try:
                # manually close old unused connection
                cls.client.close()
            except:
                logging.exception("Clearing old pymongo connection")

        cls.client = None

        # do the connection
        io_loop = ioloop.IOLoop.instance()
        conn_gr = greenlet.greenlet(_inner_connect)

        # run the connect when the ioloop starts
        io_loop.add_callback(functools.partial(conn_gr.switch,
                                               io_loop, *args, **kwargs))

        # start the ioloop
        io_loop.start()

        return cls.client
