// What the desktop window offers the UI's page, run before the page's own
// scripts (window.py appends this to Qt's qwebchannel.js). The UI asks for it
// by name (ui/imports.js, host_folders): a page in a browser has none.
//
// window.orchestrateHost.pickFolder(start) shows the OS's folder dialog, open
// at start, and resolves to the folder chosen, or "" when it is cancelled.
(() => {
  const waiting = new Map();
  let next = 1;
  const host = new Promise((resolve) => {
    new QWebChannel(qt.webChannelTransport, (channel) => {
      const h = channel.objects.host;
      h.folderPicked.connect((id, path) => {
        const done = waiting.get(id);
        waiting.delete(id);
        if (done) done(path);
      });
      resolve(h);
    });
  });
  window.orchestrateHost = {
    pickFolder: (start) => host.then((h) => new Promise((resolve) => {
      const id = next++;
      waiting.set(id, resolve);
      h.pickFolder(id, start);
    })),
  };
})();
