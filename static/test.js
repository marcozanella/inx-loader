console.log('ciao');
const protocol = window.location.protocol.includes('https') ? `wss`:`ws`;
console.log(protocol);
console.log(`WebSocket protocol: ${protocol}`);
