const log = (text) => {
    const pre = '<div class="p-1"><div class= "card card-body font-monospace font-weight-light mx-0 my-0 h6 small">'
    const post = '</div></div>'
    let log_item = pre + text + post
    // console.log('Log Item:     ' + log_item)
    // document.getElementById('log').innerHTML += `<p class="font-monospace text-trucate mx-0 my-0 small"><small>${text}</small></p>`;
    document.getElementById('log').innerHTML += log_item
    window.scrollTo(0, document.body.scrollHeight);
    console.log(text);
  };

const socket = new WebSocket('ws://' + location.host + '/echo');
  socket.addEventListener('message', ev => {
    log(ev.data);
  });

  socket.addEventListener('close', ev => {
    log("The channel was closed with message: <br>" + ev.data)
  });

  // document.getElementById('form').onsubmit = ev => {
  //  ev.preventDefault();
  //  const textField = document.getElementById('text');
  //  log('socket. send --->>> ' + textField.value);
  //  socket.send(textField.value);
  //  textField.value = '';
  // };