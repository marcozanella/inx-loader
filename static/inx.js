const log = (text) => {
    document.getElementById('log').innerHTML += `<p class="">${text}</p>`;
    
    console.log(text);
  };

const socket = new WebSocket('ws://' + location.host + '/echo');
  socket.addEventListener('message', ev => {
    log('<<<--- received from server ' + ev.data);
  });

  // document.getElementById('form').onsubmit = ev => {
  //  ev.preventDefault();
  //  const textField = document.getElementById('text');
  //  log('socket. send --->>> ' + textField.value);
  //  socket.send(textField.value);
  //  textField.value = '';
  // };