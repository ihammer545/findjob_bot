// index.js
import express from 'express';
import updateCountries from './countryUpdater.js';
import processDuplicatesAndSendWebhook from './duplicateProcessor.js';

const app = express();
const port = process.env.PORT || 10000;

app.use(express.json());

app.get('/', (req, res) => {
  res.send('✅ OK');
});

app.post('/country', async (req, res) => {
  const { date } = req.body;

  if (!date) {
    return res.status(400).send('⛔ Не указана дата');
  }

  try {
    console.log(`📅 Запуск updateCountries() для даты: ${date}`);
    const log = await updateCountries(date);
    console.log('✅ Обработка завершена. Статус:\n' + log.join('\n'));
    res.send(`<pre>${log.join('\n')}</pre>`);
  } catch (err) {
    console.error('❌ Ошибка при обработке через POST /country:', err);
    res.status(500).send('❌ Ошибка при обновлении');
  }
});

app.post('/check', async (req, res) => {
  const webhookUrl = 'https://hook.eu2.make.com/g8kdpfddlaq70l31olejqx8ibtcpba9a';
  console.log('✅ Получен запрос на проверку дублей');
  res.send('🟢 Задача принята в обработку');

  processDuplicatesAndSendWebhook(webhookUrl)
    .catch(err => console.error('❌ Ошибка фоновой задачи:', err));
});

app.listen(port, '0.0.0.0', () => {
  console.log(`✅ Server is running on http://0.0.0.0:${port}`);
});
