import time


class Chronometer:
    @staticmethod
    def makeUnstarted():
        return Chronometer()

    @staticmethod
    def makeStarted():
        return Chronometer.makeUnstarted().start()

    @staticmethod
    def show(chronometer, message):
        elapsed = chronometer.elapsed()
        Chronometer.items.append((message, elapsed))
        print("\033[32mTime: %s: %dms\033[0m" % (message, elapsed))

    items = []

    @staticmethod
    def show_resume():
        print("\033[32mResume")
        for item in Chronometer.items:
            print("%s: %dms" % item)
        print("\033[0m")

    def __init__(self):
        self.watch = Watch()
        self.startTime = 0
        self.elapsedTime = 0

    def start(self):
        self.startTime = self.watch.read()
        return self

    def stop(self):
        stopTime = self.watch.read()
        self.elapsedTime += stopTime - self.startTime
        return self

    def elapsed(self):
        return self.watch.read() - self.startTime + self.elapsedTime

    def reset(self):
        self.elapsedTime = 0
        return self


class Watch:
    def read(self):
        return int(time.time() * 1000)
