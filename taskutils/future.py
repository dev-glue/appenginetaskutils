import datetime
import functools
import hashlib
import json
import pickle
import time
import uuid
from copy import deepcopy

import cloudpickle
from google.appengine.api import taskqueue
from google.appengine.api.datastore_errors import Timeout
from google.appengine.ext import ndb

from taskutils import task
from taskutils.task import PermanentTaskFailure
from taskutils.util import logdebug, logwarning, logexception


class FutureReadyForResult(Exception):
    pass


class FutureNotReadyForResult(Exception):
    pass


class FutureTimedOutError(Exception):
    pass


class FutureCancelled(Exception):
    pass


class _FutureProgress(ndb.Model):
    localprogress = ndb.IntegerProperty()
    calculatedprogress = ndb.IntegerProperty()
    weight = ndb.IntegerProperty()


class _Future(ndb.Model):
    stored = ndb.DateTimeProperty(auto_now_add=True)
    updated = ndb.DateTimeProperty(auto_now=True)
    parentkey = ndb.KeyProperty()
    resultser = ndb.BlobProperty()
    exceptionser = ndb.BlobProperty()
    onsuccessfser = ndb.BlobProperty()
    onfailurefser = ndb.BlobProperty()
    onallchildsuccessfser = ndb.BlobProperty()
    onprogressfser = ndb.BlobProperty()
    taskkwargsser = ndb.BlobProperty()
    status = ndb.StringProperty()
    runtimesec = ndb.FloatProperty()
    initialised = ndb.BooleanProperty()
    readyforresult = ndb.BooleanProperty()
    timeoutsec = ndb.IntegerProperty()
    name = ndb.StringProperty()

    def get_taskkwargs(self, deletename=True):
        taskkwargs = pickle.loads(self.taskkwargsser)

        if deletename and "name" in taskkwargs:
            del taskkwargs["name"]

        return taskkwargs

    def intask(self, nameprefix, f, *args, **kwargs):
        taskkwargs = self.get_taskkwargs()
        name = ""
        if nameprefix:
            name = "%s-%s" % (nameprefix, self.key.id())
            taskkwargs["name"] = name
        elif taskkwargs.get("name"):
            del taskkwargs["name"]
        taskkwargs["transactional"] = False

        @task(**taskkwargs)
        def dof():
            f(*args, **kwargs)

        try:
            # run the wrapper task, and if it fails due to a name clash just skip it (it was already kicked off by an earlier
            # attempt to construct this future).
            #             logdebug("about to run task %s" % name)
            dof()
        except taskqueue.TombstonedTaskError:
            logdebug("skip adding task %s (already been run)" % name)
        except taskqueue.TaskAlreadyExistsError:
            logdebug("skip adding task %s (already running)" % name)

    def has_result(self):
        return bool(self.status)

    def get_result(self):
        if self.status == "failure":
            raise pickle.loads(self.exceptionser)
        elif self.status == "success":
            return pickle.loads(self.resultser)
        else:
            raise FutureReadyForResult("result not ready")

    def _get_progressobject(self):
        key = ndb.Key(_FutureProgress, self.key.id())
        progressobj = key.get()
        if not progressobj:
            progressobj = _FutureProgress(key=key)
        return progressobj

    def get_calculatedprogress(self, progressobj=None):
        progressobj = progressobj if progressobj else self._get_progressobject()
        return progressobj.calculatedprogress if progressobj and progressobj.calculatedprogress else 0

    def get_weight(self, progressobj=None):
        progressobj = progressobj if progressobj else self._get_progressobject()
        return progressobj.weight if progressobj and progressobj.weight else None

    def get_localprogress(self, progressobj=None):
        progressobj = progressobj if progressobj else self._get_progressobject()
        return progressobj.localprogress if progressobj and progressobj.localprogress else 0

    def _calculate_progress(self, localprogress):
        newcalculatedprogress = localprogress

        @ndb.transactional()
        def get_children_trans():
            return get_children(self.key)

        children = get_children_trans()

        if children:
            for child in children:
                newcalculatedprogress += child.get_calculatedprogress()

        return newcalculatedprogress

    #     def update_result(self):
    #         if self.readyforresult:
    #             updateresultf = UpdateResultF #pickle.loads(self.updateresultfser) if self.updateresultfser else DefaultUpdateResultF
    #             updateresultf(self)
    #
    #             # note that updateresultf can change the status
    #
    #             if self.status == "failure":
    #                 self._callOnFailure()
    #             elif self.status == "success":
    #                 self._callOnSuccess()

    def GetParent(self):
        return self.parentkey.get() if self.parentkey else None

    def GetChildren(self):
        @ndb.transactional()
        def get_children_trans():
            return get_children(self.key)

        return get_children_trans()

    def _callOnSuccess(self):
        onsuccessf = pickle.loads(self.onsuccessfser) if self.onsuccessfser else None
        if onsuccessf:
            def doonsuccessf():
                onsuccessf(self.key)

            self.intask("onsuccess", doonsuccessf)

        if self.onallchildsuccessfser:
            lparent = self.GetParent()
            if lparent and all_children_success(self.parentkey):
                onallchildsuccessf = pickle.loads(self.onallchildsuccessfser) if self.onallchildsuccessfser else None
                if onallchildsuccessf:
                    def doonallchildsuccessf():
                        onallchildsuccessf()

                    self.intask("onallchildsuccess", doonallchildsuccessf)

    def _callOnFailure(self):
        onfailuref = pickle.loads(self.onfailurefser) if self.onfailurefser else None

        def doonfailuref():
            if onfailuref:
                onfailuref(self.key)
            else:
                DefaultOnFailure(self.key)

        self.intask("onfailure", doonfailuref)

    def _callOnProgress(self):
        onprogressf = pickle.loads(self.onprogressfser) if self.onprogressfser else None
        if onprogressf:
            def doonprogressf():
                onprogressf(self.key)

            self.intask(None, doonprogressf)

    def get_runtime(self):
        if self.runtimesec:
            return datetime.timedelta(seconds=self.runtimesec)
        else:
            return datetime.datetime.utcnow() - self.stored

    def _set_local_progress_for_success(self):
        progressObj = self._get_progressobject()
        logdebug("progressObj = %s" % progressObj)
        weight = self.get_weight(progressObj)
        weight = weight or 1
        logdebug("weight = %s" % weight)
        localprogress = self.get_localprogress(progressObj)
        logdebug("localprogress = %s" % localprogress)
        if localprogress < weight and not self.GetChildren():
            logdebug("No children, we can auto set localprogress from weight")
            self.set_localprogress(weight)

    # noinspection PyProtectedMember
    @ndb.non_transactional()
    def set_success(self, result):
        key = self.key

        @ndb.transactional()
        def _set_status():
            obj = key.get()
            did_put = False
            if obj.readyforresult and not obj.status:
                obj.status = "success"
                obj.initialised = True
                obj.readyforresult = True
                obj.resultser = cloudpickle.dumps(result)
                obj.runtimesec = obj.get_runtime().total_seconds()
                did_put = True
                obj.put()
            return obj, did_put

        entity, changed = _set_status()
        if changed:
            entity._set_local_progress_for_success()
            entity._callOnSuccess()

    @ndb.non_transactional()
    def set_failure(self, exception):
        key = self.key

        @ndb.transactional()
        def _set_status():
            obj = key.get()
            did_put = False
            if not obj.status:
                obj.status = "failure"
                obj.initialised = True
                obj.readyforresult = True
                obj.exceptionser = cloudpickle.dumps(exception)
                obj.runtimesec = obj.get_runtime().total_seconds()
                did_put = True
                obj.put()
            return obj, did_put

        entity, changed = _set_status()
        if changed:
            # noinspection PyProtectedMember
            entity._callOnFailure()

            if not entity.parentkey:
                # top level. Fail everything below
                task_kwargs = entity.get_taskkwargs()

                @task(**task_kwargs)
                def fail_children(future_key):
                    children = get_children(future_key)
                    if children:
                        for child in children:
                            child.set_failure(exception)
                            fail_children(child.key)

                fail_children(entity.key)

    # noinspection PyProtectedMember
    @ndb.non_transactional()
    def set_success_and_readyforesult(self, result):
        key = self.key

        @ndb.transactional()
        def _set_status():
            obj = key.get()
            did_put = False
            if not obj.status:
                obj.status = "success"
                obj.initialised = True
                obj.readyforresult = True
                obj.resultser = cloudpickle.dumps(result)
                obj.runtimesec = obj.get_runtime().total_seconds()
                did_put = True
                obj.put()
            return obj, did_put

        entity, changed = _set_status()
        if changed:
            entity._set_local_progress_for_success()
            entity._callOnSuccess()

    @ndb.non_transactional()
    def set_readyforesult(self):
        key = self.key

        @ndb.transactional()
        def _set_status():
            obj = key.get()
            if not obj.readyforresult:
                obj.initialised = True
                obj.readyforresult = True
                obj.put()

        _set_status()

    @ndb.non_transactional()
    def set_initialised(self):
        key = self.key

        @ndb.transactional()
        def _set_status():
            obj = key.get()
            if not obj.initialised:
                obj.initialised = True
                obj.put()

        _set_status()

    def _calculate_parent_progress(self):
        parent_key = self.parentkey
        if parent_key:
            task_kwargs = self.get_taskkwargs()

            @task(**task_kwargs)
            def _parent_progress():
                parent = parent_key.get()
                if parent and isinstance(parent, _Future):
                    parent.calculate_progress()

            _parent_progress()

    def set_localprogress(self, value):
        obj = self._get_progressobject()
        local = self.get_localprogress(obj)
        calculated = self.get_calculatedprogress(obj)
        if local != value:
            #             haschildren = self.GetChildren()
            #             logdebug("haschildren: %s" % haschildren)

            obj.localprogress = value
            logdebug("localprogress: %s" % value)
            #             if not haschildren:
            changed = value > calculated
            if changed:
                logdebug("setting calculated progress")
                obj.calculatedprogress = value

            obj.put()

            if changed:
                logdebug("kicking off calculate parent progress")
                self._calculate_parent_progress()

            self._callOnProgress()

    def calculate_progress(self):
        obj = self._get_progressobject()
        local = self.get_localprogress(obj)
        calculated = self.get_calculatedprogress(obj)
        new_calculated = self._calculate_progress(local)
        if calculated != new_calculated:
            obj.calculatedprogress = new_calculated
            obj.put()
            self._calculate_parent_progress()
            self._callOnProgress()

    def set_weight(self, value):
        if value:
            progress_obj = self._get_progressobject()
            if progress_obj.weight != value:
                progress_obj.weight = value
                progress_obj.put()

    def cancel(self):
        children = get_children(self.key)
        if children:
            task_kwargs = self.get_taskkwargs()

            @task(**task_kwargs)
            def _cancel_child(child_key):
                child2 = child_key.get()
                if child2:
                    child2.cancel()

            for child in children:
                _cancel_child(child.key)

        self.set_failure(FutureCancelled("cancelled by caller"))

    def to_dict(self, level=0, max_level=5, recursive=True, future_map_fn=None):
        progress_obj = self._get_progressobject()

        children = [child.to_dict(level=level + 1, max_level=max_level, future_map_fn=future_map_fn) for child in
                    get_children(self.key)] if recursive and level + 1 < max_level else None

        result_rep = None
        result = pickle.loads(self.resultser) if self.resultser else None
        if result:
            # noinspection PyBroadException
            try:
                result_rep = result.to_dict()
            except Exception:
                # noinspection PyBroadException
                try:
                    json.dumps(result)
                    result_rep = result
                except Exception:
                    result_rep = str(result)

        if future_map_fn:
            str_key = future_map_fn(self, level)
        else:
            str_key = str(self.key) if self.key else None

        return {
            "key": str_key,
            "id": self.key.id() if self.key else None,
            "name": self.name,
            "level": level,
            "stored": str(self.stored) if self.stored else None,
            "updated": str(self.updated) if self.stored else None,
            "status": str(self.status) if self.status else "underway",
            "result": result_rep,
            "exception": repr(pickle.loads(self.exceptionser)) if self.exceptionser else None,
            "runtimesec": self.get_runtime().total_seconds(),
            "localprogress": self.get_localprogress(progress_obj),
            "progress": self.get_calculatedprogress(progress_obj),
            "weight": self.get_weight(),
            "initialised": self.initialised,
            "readyforresult": self.readyforresult,
            "zchildren": children
        }


# def UpdateResultF(futureobj):
#     if not futureobj.status and futureobj.get_runtime() > datetime.timedelta(seconds = futureobj.timeoutsec):
#         futureobj.set_failure(FutureTimedOutError("timeout"))
# 
#     taskkwargs = futureobj.get_taskkwargs()
# 
#     @task(**taskkwargs)
#     def UpdateChildren():
#         for childfuture in get_children(futureobj.key):
# #             logdebug("update_result: %s" % childfuture.key)
#             childfuture.update_result()
#     UpdateChildren()

def DefaultOnFailure(futurekey):
    futureobj = futurekey.get() if futurekey else None
    parentfutureobj = futureobj.GetParent() if futureobj else None
    if parentfutureobj and not parentfutureobj.has_result():
        if not parentfutureobj.initialised:  # or not parentfutureobj.readyforresult:
            raise Exception("Parent not initialised, retry")
        try:
            futureobj.get_result()
        except Exception, ex:
            parentfutureobj.set_failure(ex)


def GenerateOnAllChildSuccess(parentkey, initialvalue, combineresultf, failonerror=True):
    def OnAllChildSuccess():
        logdebug("Enter GenerateOnAllChildSuccess: %s" % parentkey)
        parentfuture = parentkey.get() if parentkey else None
        if parentfuture and not parentfuture.has_result():
            if not parentfuture.initialised or not parentfuture.readyforresult:
                raise Exception("Parent not initialised, retry")

            @ndb.transactional()
            def get_children_trans():
                return get_children(parentfuture.key)

            children = get_children_trans()

            logdebug("children: %s" % [child.key for child in children])
            if children:
                result = initialvalue
                error = None
                finished = True
                for childfuture in children:
                    logdebug("childfuture: %s" % childfuture.key)
                    if childfuture.has_result():
                        try:
                            childresult = childfuture.get_result()
                            logdebug("childresult(%s): %s" % (childfuture.status, childresult))
                            result = combineresultf(result, childresult)
                            logdebug("hasresult:%s" % result)
                        except Exception, ex:
                            logdebug("haserror:%s" % repr(ex))
                            error = ex
                            break
                    else:
                        logdebug("noresult")
                        finished = False

                if error:
                    logwarning("Internal error, child has error in OnAllChildSuccess: %s" % error)
                    if failonerror:
                        parentfuture.set_failure(error)
                    else:
                        raise error
                elif finished:
                    logdebug("result: %s" % result)
                    parentfuture.set_success(result)  # (result, initialamount, keyrange))
                else:
                    logdebug("child not finished in OnAllChildSuccess, skipping")
            else:
                logwarning("Internal error, parent has no children in OnAllChildSuccess")
                parentfuture.set_failure(Exception("no children found"))

    return OnAllChildSuccess


def generatefuturepagemapf(mapf, initialresult=None, oncombineresultsf=None, **taskkwargs):
    def futurepagemapf(futurekey, items):
        linitialresult = initialresult or 0
        loncombineresultsf = oncombineresultsf if oncombineresultsf else lambda a, b: a + b

        try:
            lonallchildsuccessf = GenerateOnAllChildSuccess(futurekey, linitialresult, loncombineresultsf)

            if len(items) > 5:
                leftitems = items[len(items) / 2:]
                rightitems = items[:len(items) / 2]
                future(futurepagemapf, parentkey=futurekey, futurename="split left %s" % len(leftitems),
                       onallchildsuccessf=lonallchildsuccessf, weight=len(leftitems), **taskkwargs)(leftitems)
                future(futurepagemapf, parentkey=futurekey, futurename="split right %s" % len(rightitems),
                       onallchildsuccessf=lonallchildsuccessf, weight=len(rightitems), **taskkwargs)(rightitems)
            else:
                for index, item in enumerate(items):
                    futurename = "ProcessItem %s" % index
                    future(mapf, parentkey=futurekey, futurename=futurename, onallchildsuccessf=lonallchildsuccessf,
                           weight=1, **taskkwargs)(item)
        except Exception, ex:
            raise PermanentTaskFailure(repr(ex))
        else:
            raise FutureReadyForResult()

    return futurepagemapf


def OnProgressF(futurekey):
    futureobj = futurekey.get() if futurekey else None
    if futureobj.parentkey:
        taskkwargs = futureobj.get_taskkwargs()

        logdebug("Enter OnProgressF: %s" % futureobj)

        @task(**taskkwargs)
        def UpdateParent(parentkey):
            logdebug("***************************************************")
            logdebug("Enter UpdateParent: %s" % parentkey)
            logdebug("***************************************************")

            parent = parentkey.get()
            logdebug("1: %s" % parent)
            if parent:
                logdebug("2")
                #                 if not parent.has_result():
                progress = 0
                for childfuture in get_children(parentkey):
                    logdebug("3: %s" % childfuture)
                    progress += childfuture.get_progress()
                logdebug("4: %s" % progress)
                parent.set_progress(progress)

        UpdateParent(futureobj.parentkey)


def get_children(futurekey):
    if futurekey:
        ancestorkey = ndb.Key(futurekey.kind(), futurekey.id())
        return [childfuture for childfuture in _Future.query(ancestor=ancestorkey).order(_Future.stored) if
                ancestorkey == childfuture.key.parent()]
    else:
        return []


def all_children_success(futurekey):
    lchildren = get_children(futurekey)
    retval = True
    for lchild in lchildren:
        if lchild.has_result():
            try:
                lchild.get_result()
            except Exception:
                retval = False
                break
        else:
            retval = False
            break
    return retval


def setlocalprogress(futurekey, value):
    future = futurekey.get() if futurekey else None
    if future:
        future.set_localprogress(value)


def GenerateStableId(instring):
    return hashlib.md5(instring).hexdigest()


def future(f=None, parentkey=None,
           onsuccessf=None, onfailuref=None,
           onallchildsuccessf=None,
           onprogressf=None,
           weight=None, timeoutsec=1800, maxretries=None, futurename=None, **taskkwargs):
    if not f:
        return functools.partial(future,
                                 parentkey=parentkey,
                                 onsuccessf=onsuccessf, onfailuref=onfailuref,
                                 onallchildsuccessf=onallchildsuccessf,
                                 onprogressf=onprogressf,
                                 weight=weight, timeoutsec=timeoutsec, maxretries=maxretries, futurename=futurename,
                                 **taskkwargs)

    #     logdebug("includefuturekey: %s" % includefuturekey)

    @functools.wraps(f)
    def runfuture(*args, **kwargs):
        @ndb.transactional(xg=True)
        def runfuturetrans():
            logdebug("runfuture: parentkey=%s" % parentkey)

            immediateancestorkey = ndb.Key(parentkey.kind(), parentkey.id()) if parentkey else None

            taskkwargscopy = deepcopy(taskkwargs)
            if "name" not in taskkwargscopy:
                # can only set transactional if we're not naming the task
                taskkwargscopy["transactional"] = True
                newfutureId = str(uuid.uuid4())  # id doesn't need to be stable
            else:
                # if we're using a named task, we need the key to remain stable in case of transactional retries
                # what can happen is that the task is launched, but the transaction doesn't commit. 
                # retries will then always fail to launch the task because it is already launched.
                # therefore retries need to use the same future key id, so that once this transaction does commit,
                # the earlier launch of the task will match up with it.
                taskkwargscopy["transactional"] = False
                newfutureId = GenerateStableId(taskkwargs["name"])

            newkey = ndb.Key(_Future, newfutureId, parent=immediateancestorkey)

            #         logdebug("runfuture: ancestorkey=%s" % immediateancestorkey)
            #         logdebug("runfuture: newkey=%s" % newkey)

            # just use immediate ancestor to keep entity groups at local level, not one for the entire tree
            futureobj = _Future(key=newkey)

            futureobj.parentkey = parentkey  # but keep the real parent key for lookups

            if onsuccessf:
                futureobj.onsuccessfser = cloudpickle.dumps(onsuccessf)
            if onfailuref:
                futureobj.onfailurefser = cloudpickle.dumps(onfailuref)
            if onallchildsuccessf:
                futureobj.onallchildsuccessfser = cloudpickle.dumps(onallchildsuccessf)
            if onprogressf:
                futureobj.onprogressfser = cloudpickle.dumps(onprogressf)
            futureobj.taskkwargsser = cloudpickle.dumps(taskkwargs)

            #         futureobj.onsuccessfser = yccloudpickle.dumps(onsuccessf) if onsuccessf else None
            #         futureobj.onfailurefser = yccloudpickle.dumps(onfailuref) if onfailuref else None
            #         futureobj.onallchildsuccessfser = yccloudpickle.dumps(onallchildsuccessf) if onallchildsuccessf else None
            #         futureobj.onprogressfser = yccloudpickle.dumps(onprogressf) if onprogressf else None
            #         futureobj.taskkwargsser = yccloudpickle.dumps(taskkwargs)

            #         futureobj.set_weight(weight if weight >= 1 else 1)

            futureobj.timeoutsec = timeoutsec

            futureobj.name = futurename

            futureobj.put()
            #         logdebug("runfuture: childkey=%s" % futureobj.key)

            futurekey = futureobj.key
            logdebug("outer, futurekey=%s" % futurekey)

            @task(includeheaders=True, **taskkwargscopy)
            def _futurewrapper(headers):
                if maxretries:
                    lretryCount = 0
                    try:
                        lretryCount = int(headers.get("X-Appengine-Taskretrycount", 0)) if headers else 0
                    except:
                        logexception("Failed trying to get retry count, using 0")

                    if lretryCount > maxretries:
                        raise PermanentTaskFailure("Too many retries of Future")

                logdebug("inner, futurekey=%s" % futurekey)
                futureobj2 = futurekey.get()
                if futureobj2:
                    futureobj2.set_weight(weight)  # if weight >= 1 else 1)
                else:
                    raise Exception("Future not ready yet")

                try:
                    logdebug("args, kwargs=%s, %s" % (args, kwargs))
                    result = f(futurekey, *args, **kwargs)

                except FutureReadyForResult:
                    futureobj3 = futurekey.get()
                    if futureobj3:
                        futureobj3.set_readyforesult()

                except FutureNotReadyForResult:
                    futureobj4 = futurekey.get()
                    if futureobj4:
                        futureobj4.set_initialised()

                except PermanentTaskFailure, ptf:
                    try:
                        futureobj5 = futurekey.get()
                        if futureobj5:
                            futureobj5.set_failure(ptf)
                    finally:
                        raise ptf
                else:
                    futureobj6 = futurekey.get()
                    if futureobj6:
                        futureobj6.set_success_and_readyforesult(result)

            try:
                # run the wrapper task, and if it fails due to a name clash just skip it (it was already kicked off by an earlier
                # attempt to construct this future).
                _futurewrapper()
            except taskqueue.TombstonedTaskError:
                logdebug("skip adding task (already been run)")
            except taskqueue.TaskAlreadyExistsError:
                logdebug("skip adding task (already running)")

            return futureobj

        manualRetries = 0
        while True:
            try:
                return runfuturetrans()
            except Timeout, tex:
                if manualRetries < 10:
                    manualRetries += 1
                    time.sleep(manualRetries * 5)
                else:
                    raise tex

    logdebug("about to call runfuture")
    return runfuture


def GetFutureAndCheckReady(futurekey):
    futureobj = futurekey.get() if futurekey else None
    if not (futureobj and futureobj.initialised and futureobj.readyforresult):
        raise Exception("Future not ready for result, retry")
    return futureobj


# fsf = futuresequencefunction
# different from future function because it has a "results" argument, a list.
def futuresequence(fsfseq, parentkey=None, onsuccessf=None, onfailuref=None, onallchildsuccessf=None, onprogressf=None,
                   weight=None, timeoutsec=1800, maxretries=None, futurenameprefix=None, **taskkwargs):
    logdebug("Enter futuresequence: %s" % len(fsfseq))

    flist = list(fsfseq)

    taskkwargs["futurename"] = "%s (top level)" % futurenameprefix if futurenameprefix else "sequence"

    @future(parentkey=parentkey, onsuccessf=onsuccessf, onfailuref=onfailuref, onallchildsuccessf=onallchildsuccessf,
            onprogressf=onprogressf, weight=weight, timeoutsec=timeoutsec, maxretries=maxretries, **taskkwargs)
    def toplevel(futurekey, *args, **kwargs):
        logdebug("Enter futuresequence.toplevel: %s" % futurekey)

        def childonsuccessforindex(index, results):
            logdebug("Enter childonsuccessforindex: %s, %s, %s" % (futurekey, index, json.dumps(results, indent=2)))

            def childonsuccess(childfuturekey):
                logdebug("Enter childonsuccess: %s, %s, %s" % (futurekey, index, childfuturekey))
                logdebug("results: %s" % json.dumps(results, indent=2))
                try:
                    childfuture = GetFutureAndCheckReady(childfuturekey)

                    try:
                        result = childfuture.get_result()
                    except Exception, ex:
                        toplevelfuture = futurekey.get()
                        if toplevelfuture:
                            toplevelfuture.set_failure(ex)
                        else:
                            raise Exception("Can't load toplevel future for failure")
                    else:
                        logdebug("result: %s" % json.dumps(result, indent=2))
                        newresults = results + [result]
                        islast = (index == (len(flist) - 1))

                        if islast:
                            logdebug("islast")
                            toplevelfuture = futurekey.get()
                            if toplevelfuture:
                                logdebug("setting top level success")
                                toplevelfuture.set_success_and_readyforesult(newresults)
                            else:
                                raise Exception("Can't load toplevel future for success")
                        else:
                            logdebug("not last")
                            taskkwargs["futurename"] = "%s [%s]" % (
                                futurenameprefix if futurenameprefix else "-", index + 1)
                            future(flist[index + 1], parentkey=futurekey,
                                   onsuccessf=childonsuccessforindex(index + 1, newresults),
                                   weight=weight / len(flist) if weight else None, timeoutsec=timeoutsec,
                                   maxretries=maxretries, **taskkwargs)(newresults)
                finally:
                    logdebug("Enter childonsuccess: %s, %s, %s" % (futurekey, index, childfuturekey))

            logdebug("Leave childonsuccessforindex: %s, %s, %s" % (futurekey, index, json.dumps(results, indent=2)))
            return childonsuccess

        taskkwargs["futurename"] = "%s [0]" % (futurenameprefix if futurenameprefix else "sequence")
        future(flist[0], parentkey=futurekey, onsuccessf=childonsuccessforindex(0, []),
               weight=weight / len(flist) if weight else None, timeoutsec=timeoutsec, maxretries=maxretries,
               **taskkwargs)([])  # empty list of results

        logdebug("Leave futuresequence.toplevel: %s" % futurekey)
        raise FutureNotReadyForResult("sequence started")

    return toplevel


def futureparallel(ffseq, parentkey=None, onsuccessf=None, onfailuref=None, onallchildsuccessf=None, onprogressf=None,
                   weight=None, timeoutsec=1800, maxretries=None, futurenameprefix=None, **taskkwargs):
    logdebug("Enter futureparallel: %s" % len(ffseq))
    flist = list(ffseq)

    taskkwargs["futurename"] = "%s (top level)" % futurenameprefix if futurenameprefix else "parallel"

    @future(parentkey=parentkey, onsuccessf=onsuccessf, onfailuref=onfailuref, onallchildsuccessf=onallchildsuccessf,
            onprogressf=onprogressf, weight=weight, timeoutsec=timeoutsec, maxretries=maxretries, **taskkwargs)
    def toplevel(futurekey, *args, **kwargs):
        logdebug("Enter futureparallel.toplevel: %s" % futurekey)

        def OnAllChildSuccess():
            logdebug("Enter OnAllChildSuccess: %s" % futurekey)
            parentfuture = futurekey.get() if futurekey else None
            if parentfuture and not parentfuture.has_result():
                if not parentfuture.initialised or not parentfuture.readyforresult:
                    raise Exception("Parent not initialised, retry")

                @ndb.transactional()
                def get_children_trans():
                    return get_children(parentfuture.key)

                children = get_children_trans()

                logdebug("children: %s" % [child.key for child in children])
                if children:
                    result = []
                    error = None
                    finished = True
                    for childfuture in children:
                        logdebug("childfuture: %s" % childfuture.key)
                        if childfuture.has_result():
                            try:
                                childresult = childfuture.get_result()
                                logdebug("childresult(%s): %s" % (childfuture.status, childresult))
                                result += [childfuture.get_result()]
                                logdebug("intermediate result:%s" % result)
                            except Exception, ex:
                                logdebug("haserror:%s" % repr(ex))
                                error = ex
                                break
                        else:
                            logdebug("noresult")
                            finished = False

                    if error:
                        logwarning("Internal error, child has error in OnAllChildSuccess: %s" % error)
                        parentfuture.set_failure(error)
                    elif finished:
                        logdebug("result: %s" % result)
                        parentfuture.set_success(result)
                    else:
                        logdebug("child not finished in OnAllChildSuccess, skipping")
                else:
                    logwarning("Internal error, parent has no children in OnAllChildSuccess")
                    parentfuture.set_failure(Exception("no children found"))

        for ix, ff in enumerate(flist):
            taskkwargs["futurename"] = "%s [%s]" % (futurenameprefix if futurenameprefix else "parallel", ix)
            future(ff, parentkey=futurekey, onallchildsuccessf=OnAllChildSuccess,
                   weight=weight / len(flist) if weight else None, timeoutsec=timeoutsec, maxretries=maxretries,
                   **taskkwargs)()

        logdebug("Leave futureparallel.toplevel: %s" % futurekey)
        raise FutureReadyForResult("parallel started")

    return toplevel
