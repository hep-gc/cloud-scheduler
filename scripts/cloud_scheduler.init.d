#!/bin/bash
#
# chkconfig:    345 85 15
# description:  Cloud Scheduler init script

# Source LSB function library.
if [ -r /lib/lsb/init-functions ]; then
    . /lib/lsb/init-functions
else
    exit 1
fi

# Get instance specific config file
if [ -r "/etc/sysconfig/cloud_scheduler" ]; then
    . /etc/sysconfig/cloud_scheduler
fi

# For SELinux we need to use 'runuser' not 'su'
if [ -x "/sbin/runuser" ]; then
    SU="/sbin/runuser -s /bin/sh"
else
    SU="su - -s /bin/sh"
fi

LOGFILE=$(awk '/log_location:/ {print $2}' ${CS_CONFIG_DIR}/cloud_scheduler.conf | tail -n 1)
PERSISTFILE=$(awk '/persistence_file:/ {print $2}' ${CS_CONFIG_DIR}/cloud_scheduler.conf | tail -n 1)

ok () {
    echo -e "\t\t\t\t[ " "\e[0;32mOK\e[0m" " ]"
}

start () {
    if [ -f ${PIDFILE} ]; then
        PID=`cat ${PIDFILE}`
        ps ${PID} >/dev/null
        if [ $? -eq 0 ]; then
            echo $"${SERVICE} is already running with PID ${PID}."
            return 1
        else
            echo $"${SERVICE} didn't shut down cleanly last time."
            rm -f ${PIDFILE}
        fi
    fi

    echo -n $"Starting ${SERVICE}:"
    touch  ${CRASHLOG} ${LOGFILE} ${PIDFILE} ${PERSISTFILE}
    chown ${CS_USER}:${CS_USER} ${CRASHLOG} ${LOGFILE} ${PIDFILE} ${PERSISTFILE}
    ${SU} - ${CS_USER} -c "nohup ${CS_EXEC}" </dev/null >>${CRASHLOG} 2>&1 &
    echo $! > ${PIDFILE}
    RETVAL=$?
    touch /var/lock/subsys/${SERVICE}
    chown ${CS_USER}:${CS_USER} /var/lock/subsys/${SERVICE}    
    ok
}

stop () {
    if [ -f ${PIDFILE} ]; then
        echo -n $"Stopping ${SERVICE}:"
        PID=`cat ${PIDFILE}`
        kill ${PID}
        while ps -p ${PID} >/dev/null; do sleep 1; done
        if [ $? -eq 0 ]; then
            rm -f ${PIDFILE} >/dev/null 2>&1
        fi
        rm -f /var/lock/subsys/${SERVICE}
        ok
    fi
}

forcekill () {
    if [ -f $PIDFILE ]; then
        echo -n $"Killing ${SERVICE}:"
        kill -9 `cat ${PIDFILE}`
        if [ $? -eq 0 ]; then
            rm -f ${PIDFILE} >/dev/null 2>&1
        fi
        ok
        rm -f /var/lock/subsys/${SERVICE}
    fi
}

quickstop () {
    if [ -f ${PIDFILE} ]; then
        echo -n $"Setting Quick Exit Flag for ${SERVICE}:"
        kill -s SIGUSR2 `cat ${PIDFILE}`
        ok
        stop
    fi
}

reconfig () {
    if [ -f ${PIDFILE} ]; then
        echo -n $"Reconfiguring ${SERVICE}:"
        kill -s SIGUSR1 `cat ${PIDFILE}`
        ok
    fi
}


RETVAL=0

case "$1" in
    start)
        start
        ;;
    stop)
        quickstop
        ;;
    fullstop)
        stop
        ;;
    forcekill)
        forcekill
        ;;
    reconfig)
        echo $"reconfig disabled. Use quickrestart"
        #reconfig
        ;;
    status)
        if [ -f ${PIDFILE} ]; then
            PID=`cat ${PIDFILE}`
            ps $PID >/dev/null
            if [ $? -eq 0 ]; then
                PID=`cat ${PIDFILE}`
                echo $"${SERVICE} is running with PID ${PID}."
            else
                echo $"${SERVICE} has exited unexpectedly."
            fi
        else
            echo "${SERVICE} isn't running."
        fi
        ;;
    restart)
        quickstop
        start
        ;;
    quickstop)
        quickstop
        ;;
    quickrestart)
        quickstop
        start
        ;;
    reload)
        quickstop
        start
        ;;
    force-reload)
        quickstop
        start
        ;;
    *)
        echo $"Usage: $0 {start|stop|restart|status|reconfig|forcekill|quickstop|quickrestart}"
        exit 3
        ;;
esac

exit ${RETVAL}
