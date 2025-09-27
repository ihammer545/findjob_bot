// index.js
import express from 'express';
import updateCountries from './countryUpdater.js';
import processDuplicatesAndSendWebhook from './duplicateProcessor.js';

// ⬇️ ДОБАВИТЬ: импорт приложения/роутера masschange
// ВАРИАНТ A: если в masschange.js экспортируется `export default app`
import masschangeApp from './masschange.js';

// ВАРИАНТ B: если в masschange.js экспортируется хендлер `export const massChangeHandler = ...`
// import { massChangeHandler } from './masschange.js';

const app = express();
const port = process.env.PORT || 10000;

app.use(express.json());

// ⬇️ ДОБАВИТЬ: монтируем маршруты masschange
// ВАРИАНТ A (default app):
app.use(masschangeApp);

// ВАРИАНТ B (handler):
// app.all('/masschange', massChangeHandler);

app.get('/', (req, res) => {
  res.send('✅ OK');
});

app.post('/country', async (req, res) => {
  const { date, alldates, id } = req.body;

  if (id) {
    console.log(`📌 Обработка по ID: ${id}`);
    res.status(202).send(`🟢 Задача принята. Обработка строки с ID: ${id}`);
    try {
      await updateCountries(null, false, id);
    } catch (err) {
      console.error('❌ Ошибка в updateCountries:', err);
    }
    return;
  }

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
