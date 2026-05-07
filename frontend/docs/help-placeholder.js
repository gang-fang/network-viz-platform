(function () {
  const MESSAGE_TEXT = 'The chatbot is currently being trained.';

  document.querySelectorAll('.help-placeholder').forEach(link => {
    link.addEventListener('click', event => {
      event.preventDefault();

      const parent = link.parentElement;
      if (!parent || parent.querySelector('.chatbot-training-message')) return;

      const message = document.createElement('div');
      message.className = 'chatbot-training-message';
      message.textContent = MESSAGE_TEXT;
      parent.appendChild(message);
    });
  });
}());
