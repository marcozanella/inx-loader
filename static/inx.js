const log = (text) => {
    // Protocol of communication and tagging
    // badge_string***message_string
    // console.log('Message is - ' + text);
    const parts = text.split('***');
    console.log ('Length = ' + parts.length.toString())
    if (parts.length === 2) {
      var [badge, message] = parts;
      // console.log('Badge:', badge);
      // console.log('Message:', message);
    } else {
      badge = '';
      console.log('entrati qui nell 1');
      var message = parts;
      // console.log('No Badge - ' + badge);
      // console.log('Message:', message);
    }

    var card_open = '<div class="card mb-1">';
    if (parts.length === 2) {
      var card_header_open = '<div class="card-header p-1">';
      var card_header_close = '</div>';
    }
    else {
      var card_header_open = '';
      var card_header_close = '';
    }
    var card_body_open = '<div class= "card-body font-monospace font-weight-light small">';
    var badge_pre = '<span class="badge text-bg-primary">';
    var badge_post = '</span>';
    if (badge !== '') {
      var badge_span = badge_pre + badge.toUpperCase() + badge_post
    } else {
      var badge_span = ''
    };
    
    var card_body_close = '</div>';
    var card_close = '</div>';

    var log_item = card_open + card_header_open + badge_span + card_header_close + card_body_open + message + card_body_close + card_close;
    // var log_item = card_open + card_header_open + badge_span + + card_header_close + card_body_open + message + card_body_close + card_close;
    // console.log(log_item)
    document.getElementById('log').innerHTML += log_item
    window.scrollTo(0, document.body.scrollHeight);
  };

const protocol = window.location.protocol.includes('https') ? `wss`:`ws`;
console.log(`protocol: ${protocol}`);

// const socket = new WebSocket('ws://' + location.host + '/echo');
const socket = new WebSocket(`${protocol}://${location.host}/echo`);

  socket.addEventListener('message', ev => {
    log(ev.data);
  });

  socket.addEventListener('close', ev => {
    log("The channel with the backend is closed<br><br>message content:<br>" + ev.data)
  });