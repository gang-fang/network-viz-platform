function validateString(value, fieldName) {
  if (typeof value !== 'string' || value.trim() === '') {
    return `${fieldName} must be a non-empty string`;
  }

  return null;
}

function validateStringArray(value, fieldName, maxItems) {
  if (!Array.isArray(value)) {
    return `${fieldName} must be an array`;
  }

  if (value.length > maxItems) {
    return `${fieldName} is limited to ${maxItems} items`;
  }

  if (!value.every(item => typeof item === 'string' && item.trim() !== '')) {
    return `${fieldName} must contain only non-empty strings`;
  }

  return null;
}

function sendValidationError(res, message) {
  return res.status(400).json({ error: message });
}

module.exports = {
  validateString,
  validateStringArray,
  sendValidationError,
};
