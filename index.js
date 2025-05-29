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
  const { date, alldates } = req.body;
  const isAllDates = alldates === true || alldates === 'true';

  if (!isAllDates && !date) {
    return res.status(400).send('❌ Не передана дата и не указан флаг alldates');
  }

  console.log(`📅 Получен POST /country с датой: ${date} (alldates: ${isAllDates})`);
  res.status(202).send(`🟢 Задача принята. Обработка: ${isAllDates ? 'все даты' : date}`);

  try {
    await updateCountries(date, isAllDates);
  } catch (err) {
    console.error('❌ Ошибка в updateCountries:', err);
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
